@testitem "Basic Stage exceptions" setup=[InitializeObjectStore, SnowflakeMock] begin
    using CloudBase.CloudTest: Minio, Azurite
    import CloudBase
    using RustyObjectStore: RustyObjectStore, ClientOptions, SnowflakeConfig, AbstractConfig
    using RustyObjectStore: get_object!, put_object, delete_object

    function run_basic_test_cases(config::AbstractConfig, provider::Symbol)
        @testset "Insufficient output buffer size" begin
            input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
            buffer = Vector{UInt8}(undef, 10)
            @assert sizeof(input) == 100
            @assert sizeof(buffer) < sizeof(input)

            nbytes_written = put_object(codeunits(input), "test100B.csv", config)
            @test nbytes_written == 100

            try
                nbytes_read = get_object!(buffer, "test100B.csv", config)
                @test false # Should have thrown an error
            catch err
                @test err isa RustyObjectStore.GetException
                @test occursin("Supplied buffer was too small", err.msg)
            end
        end

        @testset "Non-existing file" begin
            buffer = Vector{UInt8}(undef, 100)
            try
                get_object!(buffer, "doesnt_exist.csv", config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("404 Not Found", e.msg)
                println("type of config: ", typeof(config))
                println("config: ", config)
                if provider == :aws
                    @test occursin("The specified key does not exist", e.msg)
                else
                    @test occursin("The specified blob does not exist", e.msg)
                end
            end
        end

        @testset "Delete non-existing file" begin
            if provider == :aws
                # S3 semantics is to return success on deleting a non-existing file, so we expect this
                # to succeed
                delete_object("doesnt_exist.csv", config)
                @test true
            else
                try
                    delete_object("doesnt_exist.csv", config)
                    @test false # Should have thrown an error
                catch e
                    @test e isa RustyObjectStore.DeleteException
                    @test occursin("404 Not Found", e.msg)
                    @test occursin("The specified blob does not exist", e.msg)
                end
            end
        end

        @testset "Non-existing container" begin
            bad_config = SnowflakeConfig(
                stage="doesnotexist",
                account=config.account,
                database=config.database,
                schema=config.schema,
                endpoint=config.endpoint,
                master_token_path=config.master_token_path,
                opts=ClientOptions(max_retries=2)
            )
            buffer = Vector{UInt8}(undef, 100)

            try
                put_object(codeunits("a,b,c"), "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("Stage not found", e.msg)
            end

            nbytes_written = put_object(codeunits("a,b,c"), "invalid_credentials2.csv", config)
            @assert nbytes_written == 5

            try
                get_object!(buffer, "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("Stage not found", e.msg)
            end
        end
    end

    function run_stale_config_test_cases(stale_config::AbstractConfig)
        @testset "Connection error" begin
            buffer = Vector{UInt8}(undef, 100)
            # These test retry the connection error
            try
                put_object(codeunits("a,b,c"), "still_doesnt_exist.csv", stale_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("Connection refused", e.msg) || occursin("Unable to access master token file", e.msg)
            end

            try
                get_object!(buffer, "still_doesnt_exist.csv", stale_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("Connection refused", e.msg) || occursin("Unable to access master token file", e.msg)
            end
        end
    end

    @testset "aws" begin
        # For interactive testing, use Minio.run() instead of Minio.with()
        # conf, p = Minio.run(; debug=true, public=false); atexit(() -> kill(p))
        Minio.with(; debug=true, public=false) do conf
            credentials, container = conf
            with(SFGatewayMock(credentials, container, true)) do config::SnowflakeConfig
                global _stale_config = config

                run_basic_test_cases(config, :aws)

            end # with(SFGatewayMock)
        end # Minio.with

        # MinIO is not running at this point
        run_stale_config_test_cases(_stale_config)
    end

    @testset "azure" begin
        # For interactive testing, use Azurite.run() instead of Azurite.with()
        # conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
        Azurite.with(; debug=true, public=false) do conf
            credentials, container = conf
            with(SFGatewayMock(credentials, container, true)) do config::SnowflakeConfig
                global _stale_config = config

                run_basic_test_cases(config, :azure)

            end # with(SFGatewayMock)
        end # Azurite.with

        # Azurite is not running at this point
        run_stale_config_test_cases(_stale_config)
    end


    @testset "multiple start" begin
        res = @ccall RustyObjectStore.rust_lib.start()::Cint
        @test res == 1 # Rust CResult::Error
    end
end # @testitem

@testitem "Snowflake Stage retries" setup=[InitializeObjectStore, SnowflakeMock] begin
    using CloudBase.CloudTest: Minio
    import CloudBase
    using RustyObjectStore: get_object!, put_object, SnowflakeConfig, ClientOptions, is_timeout, is_early_eof, status_code
    import HTTP
    import Sockets

    max_retries = 2
    retry_timeout_secs = 10
    request_timeout_secs = 1

    function for_all_providers(f::Function)
        for provider in [:aws, :azure]
            @testset "$(String(provider))" begin
                f(provider)
            end
        end
    end

    function get_creds_and_container(provider, port)
        @assert provider === :aws || provider === :azure
        if provider == :aws
            return (
                CloudBase.AWSCredentials("dummy", "dummy"),
                CloudBase.AWS.Bucket("dummy", host="http://127.0.0.1:$port"),
            )
        else
            return (
                CloudBase.AzureCredentials("dummy", "dummy"),
                CloudBase.Azure.Container("dummy", "dummy", host="http://127.0.0.1:$port"),
            )
        end
    end

    # Starts a TCP server that will accept the connection and start reading a few
    # bytes before closing the connection.
    function test_tcp_error(method, provider)
        @assert method === :GET || method === :PUT
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8082)
        @async begin
            while true
                sock = Sockets.accept(tcp_server)
                _ = read(sock, 4)
                close(sock)
                nrequests[] += 1
            end
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, true, opts=opts)) do conf::SnowflakeConfig
            try
                method === :GET && get_object!(zeros(UInt8, 5), "blob", conf)
                method === :PUT && put_object(codeunits("a,b,c"), "blob", conf)
                @test false # Should have thrown an error
            catch e
                method === :GET && @test e isa RustyObjectStore.GetException
                method === :PUT && @test e isa RustyObjectStore.PutException
                @test occursin("connection closed", e.msg)
                @test is_early_eof(e)
            finally
                close(tcp_server)
            end
        end
        return nrequests[]
    end

    # Starts an HTTP server that will respond with the headers for a GET response
    # and will write a partial body before closing the connection.
    function test_get_stream_error(provider)
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8083)
        http_server = HTTP.listen!(tcp_server) do http::HTTP.Stream
            nrequests[] += 1
            HTTP.setstatus(http, 200)
            HTTP.setheader(http, "Content-Length" => "20")
            if provider == :azure
                HTTP.setheader(http, "Last-Modified" => "Tue, 15 Oct 2019 12:45:26 GMT")
                HTTP.setheader(http, "ETag" => "123")
            end
            HTTP.startwrite(http)
            write(http, "not enough")
            close(http.stream)
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, false, opts=opts)) do conf::SnowflakeConfig
            try
                get_object!(zeros(UInt8, 20), "blob", conf)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("end of file before message length reached", e.msg)
                @test is_early_eof(e)
            finally
                close(http_server)
            end
            wait(http_server)
        end
        return nrequests[]
    end

    function dummy_cb(handle::Ptr{Cvoid})
        return nothing
    end

    # Starts a TCP server that will accept the connection and start reading a few
    # bytes before forcing a TCP reset on the connection.
    function test_tcp_reset(method, provider)
        @assert method === :GET || method === :PUT
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8082)
        @async begin
            while true
                sock = Sockets.accept(tcp_server)
                _ = read(sock, 4)
                nrequests[] += 1
                ccall(
                    :uv_tcp_close_reset,
                    Cint,
                    (Ptr{Cvoid}, Ptr{Cvoid}),
                    sock.handle, @cfunction(dummy_cb, Cvoid, (Ptr{Cvoid},))
                )
            end
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, true, opts=opts)) do conf::SnowflakeConfig
            try
                method === :GET && get_object!(zeros(UInt8, 5), "blob", conf)
                method === :PUT && put_object(codeunits("a,b,c"), "blob", conf)
                @test false # Should have thrown an error
            catch e
                method === :GET && @test e isa RustyObjectStore.GetException
                method === :PUT && @test e isa RustyObjectStore.PutException
                @test occursin("reset by peer", e.msg)
                @test is_connection(e)
            finally
                close(tcp_server)
            end
        end
        return nrequests[]
    end

    # Starts an HTTP server that will respond with the headers for a GET response
    # and will write a partial body before forcing a TCP reset on the connection.
    function test_get_stream_reset(provider)
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8083)
        http_server = HTTP.listen!(tcp_server) do http::HTTP.Stream
            nrequests[] += 1
            HTTP.setstatus(http, 200)
            HTTP.setheader(http, "Content-Length" => "20")
            if provider == :azure
                HTTP.setheader(http, "Last-Modified" => "Tue, 15 Oct 2019 12:45:26 GMT")
                HTTP.setheader(http, "ETag" => "123")
            end
            HTTP.startwrite(http)
            write(http, "not enough")
            socket = HTTP.IOExtras.tcpsocket(HTTP.Connections.getrawstream(http))
            ccall(
                :uv_tcp_close_reset,
                Cint,
                (Ptr{Cvoid}, Ptr{Cvoid}),
                socket.handle, @cfunction(dummy_cb, Cvoid, (Ptr{Cvoid},))
            )
            close(http.stream)
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, false, opts=opts)) do conf::SnowflakeConfig
            try
                get_object!(zeros(UInt8, 20), "blob", conf)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("Connection reset by peer", e.msg)
                @test is_early_eof(e)
            finally
                Threads.@spawn HTTP.forceclose(http_server)
            end
        end
        # wait(http_server)
        return nrequests[]
    end

    # Starts an HTTP server that will respond with the headers for a GET response
    # and will write a partial body before sleeping until the client times out.
    function test_get_stream_timeout(provider)
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8083)
        http_server = HTTP.listen!(tcp_server) do http::HTTP.Stream
            nrequests[] += 1
            HTTP.setstatus(http, 200)
            HTTP.setheader(http, "Content-Length" => "20")
            HTTP.setheader(http, "Last-Modified" => "Tue, 15 Oct 2019 12:45:26 GMT")
            HTTP.setheader(http, "ETag" => "123")
            HTTP.startwrite(http)
            write(http, "not enough")
            sleep(10)
            close(http.stream)
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs,
            request_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, false, opts=opts)) do conf::SnowflakeConfig
            try
                get_object!(zeros(UInt8, 20), "blob", conf)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("operation timed out", e.msg)
                @test is_timeout(e)
            finally
                Threads.@spawn HTTP.forceclose(http_server)
            end
        end
        # wait(http_server)
        return nrequests[]
    end

    # Starts an HTTP server that will respond with the provided status code
    # when receiving a request for the provided method, it optionally returns
    # the provided headers in the response
    function test_status(method, response_status, provider, headers=nothing)
        @assert method === :GET || method === :PUT
        nrequests = Ref(0)
        response_body = "response body from the dummy server"

        (port, tcp_server) = Sockets.listenany(8081)
        http_server = HTTP.serve!(tcp_server) do request::HTTP.Request
            if request.method == "GET" && request.target == "/$container/_this_file_does_not_exist"
                # This is the exploratory ping from connect_and_test in lib.rs
                return HTTP.Response(404, "Yup, still doesn't exist")
            end
            nrequests[] += 1
            response = isnothing(headers) ? HTTP.Response(response_status, response_body) : HTTP.Response(response_status, headers, response_body)
            return response
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, true, opts=opts)) do conf::SnowflakeConfig
            try
                method === :GET && get_object!(zeros(UInt8, 5), "blob", conf)
                method === :PUT && put_object(codeunits("a,b,c"), "blob", conf)
                @test false # Should have thrown an error
            catch e
                method === :GET && @test e isa RustyObjectStore.GetException
                method === :PUT && @test e isa RustyObjectStore.PutException
                @test occursin(string(response_status), e.msg)
                @test status_code(e) == response_status
                response_status < 500 && (@test occursin("response body from the dummy server", e.msg))
            finally
                close(http_server)
            end
            wait(http_server)
        end
        return nrequests[]
    end

    # Starts an HTTP server that upon receiving the request sleeps until the client times out.
    function test_timeout(method, message, provider, wait_secs::Int = 60)
        @assert method === :GET || method === :PUT
        nrequests = Ref(0)
        response_body = "response body from the dummy server"

        (port, tcp_server) = Sockets.listenany(8081)
        http_server = HTTP.serve!(tcp_server) do request::HTTP.Request
            if request.method == "GET" && request.target == "/$container/_this_file_does_not_exist"
                # This is the exploratory ping from connect_and_test in lib.rs
                return HTTP.Response(404, "Yup, still doesn't exist")
            end
            nrequests[] += 1
            if wait_secs > 0
                sleep(wait_secs)
            end
            return HTTP.Response(200, response_body)
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=retry_timeout_secs,
            request_timeout_secs
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, true, opts=opts)) do conf::SnowflakeConfig
            try
                method === :GET && get_object!(zeros(UInt8, 5), "blob", conf)
                method === :PUT && put_object(codeunits("a,b,c"), "blob", conf)
                @test false # Should have thrown an error
            catch e
                method === :GET && @test e isa RustyObjectStore.GetException
                method === :PUT && @test e isa RustyObjectStore.PutException
                @test is_timeout(e)
                @test occursin(string(message), e.msg)
            finally
                close(http_server)
            end
            wait(http_server)
        end
        return nrequests[]
    end

    # Starts an HTTP server that upon receiving the request sleeps for 5 seconds to allow
    # for the client to simulate a cancellation.
    function test_cancellation(provider)
        nrequests = Ref(0)
        response_body = "response body from the dummy server"

        (port, tcp_server) = Sockets.listenany(8081)
        http_server = HTTP.serve!(tcp_server) do request::HTTP.Request
            if request.method == "GET" && request.target == "/$container/_this_file_does_not_exist"
                # This is the exploratory ping from connect_and_test in lib.rs
                return HTTP.Response(404, "Yup, still doesn't exist")
            end
            nrequests[] += 1
            sleep(5)
            return HTTP.Response(200, response_body)
        end

        opts = ClientOptions(;
            max_retries=max_retries,
            retry_timeout_secs=10,
            request_timeout_secs=10
        )
        credentials, container = get_creds_and_container(provider, port)
        with(SFGatewayMock(credentials, container, true, opts=opts)) do conf::SnowflakeConfig
            try
                size = 7_000_000
                ptr = Base.Libc.malloc(size)
                buf = unsafe_wrap(Array, convert(Ptr{UInt8}, ptr), size)
                t = errormonitor(Threads.@spawn begin
                    try
                        RustyObjectStore.put_object(buf, "cancelled.bin", conf)
                        @test false
                    catch e
                        @test e == "cancel"
                    finally
                        Base.Libc.free(ptr)
                    end

                    true
                end)
                sleep(1)
                schedule(t, "cancel"; error=true)
                @test fetch(t::Task)
            finally
                HTTP.forceclose(http_server)
            end
            wait(http_server)
        end
        return nrequests[]
    end

    @testset "400: Bad Request" begin
        # Returned when there's an error in the request URI, headers, or body. The response body
        # contains an error message explaining what the specific problem is.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # AWS S3 can also respond with this code for other unrecoverable cases such as when
        # an upload exceeds the maximum allowed object size
        # See https://www.rfc-editor.org/rfc/rfc9110#status.400
        for_all_providers() do provider
            nrequests = test_status(:GET, 400, provider)
            @test nrequests == 1
            nrequests = test_status(:PUT, 400, provider)
            @test nrequests == 1
        end
    end

    @testset "403: Forbidden" begin
        # Returned when you pass an invalid api-key.
        # See https://www.rfc-editor.org/rfc/rfc9110#status.403
        for_all_providers() do provider
            nrequests = test_status(:GET, 403, provider)
            @test nrequests == 1
            nrequests = test_status(:PUT, 403, provider)
            @test nrequests == 1
        end
    end

    @testset "404: Not Found" begin
        # Returned when container not found or blob not found
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.404
        for_all_providers() do provider
            nrequests = test_status(:GET, 404, provider)
            @test nrequests == 1
        end
    end

    @testset "405: Method Not Supported" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.405
        for_all_providers() do provider
            nrequests = test_status(:GET, 405, provider, ["Allow" => "PUT"])
            @test nrequests == 1
            nrequests = test_status(:PUT, 405, provider, ["Allow" => "GET"])
            @test nrequests == 1
        end
    end

    @testset "409: Conflict" begin
        # Returned when write operations conflict.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.409
        for_all_providers() do provider
            nrequests = test_status(:GET, 409, provider)
            @test nrequests == 1
            nrequests = test_status(:PUT, 409, provider)
            @test nrequests == 1
        end
    end

    @testset "412: Precondition Failed" begin
        # Returned when an If-Match or If-None-Match header's condition evaluates to false
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.412
        for_all_providers() do provider
            nrequests = test_status(:GET, 412, provider)
            @test nrequests == 1
            nrequests = test_status(:PUT, 412, provider)
            @test nrequests == 1
        end
    end

    @testset "413: Content Too Large" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.413
        for_all_providers() do provider
            nrequests = test_status(:PUT, 413, provider)
            @test nrequests == 1
        end
    end

    @testset "429: Too Many Requests" begin
        for_all_providers() do provider
            # See https://www.rfc-editor.org/rfc/rfc6585#section-4
            nrequests = test_status(:GET, 429, provider)
            @test nrequests == 1
            nrequests = test_status(:PUT, 429, provider)
            @test nrequests == 1
            # See https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
            # TODO: We probably should respect the Retry-After header, but we currently don't
            # (and we don't know if AWS actually sets it)
            # This can happen when AWS is throttling us, so it might be a good idea to retry with some
            # larger initial backoff (very eager retries probably only make the situation worse).
            nrequests = test_status(:GET, 429, provider, ["Retry-After" => 10])
            @test nrequests == 1 + max_retries broken=true
            nrequests = test_status(:PUT, 429, provider, ["Retry-After" => 10])
            @test nrequests == 1 + max_retries broken=true
        end
    end

    @testset "502: Bad Gateway" begin
        # https://www.rfc-editor.org/rfc/rfc9110#status.502
        #   The 502 (Bad Gateway) status code indicates that the server, while acting as a
        #   gateway or proxy, received an invalid response from an inbound server it accessed
        #   while attempting to fulfill the request.
        # This error can occur when you enter HTTP instead of HTTPS in the connection.
        for_all_providers() do provider
            nrequests = test_status(:GET, 502, provider)
            @test nrequests == 1 + max_retries
            nrequests = test_status(:PUT, 502, provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "503: Service Unavailable" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.503
        #   The 503 (Service Unavailable) status code indicates that the server is currently
        #   unable to handle the request due to a temporary overload or scheduled maintenance,
        #   which will likely be alleviated after some delay. The server MAY send a Retry-After
        #   header field (Section 10.2.3) to suggest an appropriate amount of time for the
        #   client to wait before retrying the request.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        for_all_providers() do provider
            nrequests = test_status(:GET, 503, provider)
            @test nrequests == 1 + max_retries
            nrequests = test_status(:PUT, 503, provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "504: Gateway Timeout" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.504
        #   The 504 (Gateway Timeout) status code indicates that the server, while acting as
        #   a gateway or proxy, did not receive a timely response from an upstream server it
        #   needed to access in order to complete the request
        for_all_providers() do provider
            nrequests = test_status(:GET, 504, provider)
            @test nrequests == 1 + max_retries
            nrequests = test_status(:PUT, 504, provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "Timeout" begin
        for_all_providers() do provider
            nrequests = test_timeout(:GET, "timed out", provider, 2)
            @test nrequests == 1 + max_retries
            nrequests = test_timeout(:PUT, "timed out", provider, 2)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "TCP Closed" begin
        for_all_providers() do provider
            nrequests = test_tcp_error(:GET, provider)
            @test nrequests == 1 + max_retries
            nrequests = test_tcp_error(:PUT, provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "TCP reset" begin
        for_all_providers() do provider
            nrequests = test_tcp_reset(:GET, provider)
            @test nrequests == 1 + max_retries
            nrequests = test_tcp_reset(:PUT, provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "Incomplete GET body" begin
        for_all_providers() do provider
            nrequests = test_get_stream_error(provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "Incomplete GET body reset" begin
        for_all_providers() do provider
            nrequests = test_get_stream_reset(provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "Incomplete GET body timeout" begin
        for_all_providers() do provider
            nrequests = test_get_stream_timeout(provider)
            @test nrequests == 1 + max_retries
        end
    end

    @testset "Cancellation" begin
        for_all_providers() do provider
            nrequests = test_cancellation(provider)
            @test nrequests == 1
        end
    end
end
