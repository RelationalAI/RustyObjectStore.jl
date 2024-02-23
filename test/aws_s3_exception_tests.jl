@testitem "Basic S3 exceptions" setup=[InitializeObjectStore] begin
    using CloudBase.CloudTest: Minio
    import CloudBase
    using RustyObjectStore: RustyObjectStore, get_object!, put_object, ClientOptions, AWSConfig

    # For interactive testing, use Minio.run() instead of Minio.with()
    # conf, p = Minio.run(; debug=true, public=false); atexit(() -> kill(p))
    Minio.with(; debug=true, public=false) do conf
        _credentials, _container = conf
        base_url = _container.baseurl
        default_region = "us-east-1"
        config = AWSConfig(;
            region=default_region,
            bucket_name=_container.name,
            access_key_id=_credentials.access_key_id,
            secret_access_key=_credentials.secret_access_key,
            host=base_url
        )
        global _stale_config = config
        global _stale_base_url = base_url

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
                @test err.msg == "failed to process get with error: Supplied buffer was too small"
            end
        end

        @testset "Malformed credentials" begin
            input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
            buffer = Vector{UInt8}(undef, 100)
            bad_config = AWSConfig(;
                region=default_region,
                bucket_name=_container.name,
                access_key_id=_credentials.access_key_id,
                secret_access_key="",
                host=base_url
            )

            try
                put_object(codeunits(input), "invalid_credentials.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("403 Forbidden", e.msg)
                @test occursin("Check your key and signing method", e.msg)
            end

            nbytes_written = put_object(codeunits(input), "invalid_credentials.csv", config)
            @assert nbytes_written == 100

            try
                get_object!(buffer, "invalid_credentials.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("403 Forbidden", e.msg)
                @test occursin("Check your key and signing method", e.msg)
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
                @test occursin("The specified key does not exist", e.msg)
            end
        end

        @testset "Non-existing container" begin
            non_existent_container_name = string(_container.name, "doesntexist")
            non_existent_base_url = replace(base_url, _container.name => non_existent_container_name)
            bad_config = AWSConfig(;
                region=default_region,
                bucket_name=non_existent_container_name,
                access_key_id=_credentials.access_key_id,
                secret_access_key=_credentials.secret_access_key,
                host=non_existent_base_url
            )
            buffer = Vector{UInt8}(undef, 100)

            try
                put_object(codeunits("a,b,c"), "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified bucket does not exist", e.msg)
            end

            nbytes_written = put_object(codeunits("a,b,c"), "invalid_credentials2.csv", config)
            @assert nbytes_written == 5

            try
                get_object!(buffer, "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified bucket does not exist", e.msg)
            end
        end
    end # Minio.with
    # Minio is not running at this point
    @testset "Connection error" begin
        buffer = Vector{UInt8}(undef, 100)
        # These test retry the connection error
        try
            put_object(codeunits("a,b,c"), "still_doesnt_exist.csv", _stale_config)
            @test false # Should have thrown an error
        catch e
            @test e isa RustyObjectStore.PutException
            @test occursin("Connection refused", e.msg)
        end

        try
            get_object!(buffer, "still_doesnt_exist.csv", _stale_config)
            @test false # Should have thrown an error
        catch e
            @test e isa RustyObjectStore.GetException
            @test occursin("Connection refused", e.msg)
        end
    end

    @testset "multiple start" begin
        res = @ccall RustyObjectStore.rust_lib.start()::Cint
        @test res == 1 # Rust CResult::Error
    end
end # @testitem

### See AWS S3 docs:
### - "Error Responses - Amazon S3":
###   https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
### - "GetObject"
###  https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
### - "PutObject"
###  https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
@testitem "AWS S3 retries" setup=[InitializeObjectStore] begin
    using CloudBase.CloudTest: Minio
    import CloudBase
    using RustyObjectStore: get_object!, put_object, AWSConfig, ClientOptions, is_timeout, is_early_eof, status_code
    import HTTP
    import Sockets

    max_retries = 2
    retry_timeout_secs = 10
    request_timeout_secs = 1
    region = "us-east-1"
    container = "mybucket"
    dummy_access_key_id = "qUwJPLlmEtlCDXJ1OUzF"
    dummy_secret_access_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    function test_tcp_error(method)
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

        baseurl = "http://127.0.0.1:$port"
        conf = AWSConfig(;
            region=region,
            bucket_name=container,
            access_key_id=dummy_access_key_id,
            secret_access_key=dummy_secret_access_key,
            host=baseurl,
            opts=ClientOptions(;
                max_retries=max_retries,
                retry_timeout_secs=retry_timeout_secs
            )
        )

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
        return nrequests[]
    end

    function test_get_stream_error()
        nrequests = Ref(0)

        (port, tcp_server) = Sockets.listenany(8083)
        http_server = HTTP.listen!(tcp_server) do http::HTTP.Stream
            nrequests[] += 1
            HTTP.setstatus(http, 200)
            HTTP.setheader(http, "Content-Length" => "20")
            HTTP.startwrite(http)
            write(http, "not enough")
            close(http.stream)
        end

        baseurl = "http://127.0.0.1:$port"
        conf = AWSConfig(;
            region=region,
            bucket_name=container,
            access_key_id=dummy_access_key_id,
            secret_access_key=dummy_secret_access_key,
            host=baseurl,
            opts=ClientOptions(;
                max_retries=max_retries,
                retry_timeout_secs=retry_timeout_secs
            )
        )

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
        return nrequests[]
    end

    function test_status(method, response_status, headers=nothing)
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

        baseurl = "http://127.0.0.1:$port"
        conf = AWSConfig(;
            region=region,
            bucket_name=container,
            access_key_id=dummy_access_key_id,
            secret_access_key=dummy_secret_access_key,
            host=baseurl,
            opts=ClientOptions(;
                max_retries=max_retries,
                retry_timeout_secs=retry_timeout_secs
            )
        )

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
        return nrequests[]
    end

    function test_timeout(method, message, wait_secs::Int = 60)
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

        baseurl = "http://127.0.0.1:$port"
        conf = AWSConfig(;
            region=region,
            bucket_name=container,
            access_key_id=dummy_access_key_id,
            secret_access_key=dummy_secret_access_key,
            host=baseurl,
            opts=ClientOptions(;
                max_retries=max_retries,
                retry_timeout_secs=retry_timeout_secs,
                request_timeout_secs
            )
        )

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
        return nrequests[]
    end

    @testset "400: Bad Request" begin
        # Returned when there's an error in the request URI, headers, or body. The response body
        # contains an error message explaining what the specific problem is.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # AWS S3 can also respond with this code for other unrecoverable cases such as when
        # an upload exceeds the maximum allowed object size
        # See https://www.rfc-editor.org/rfc/rfc9110#status.400
        nrequests = test_status(:GET, 400)
        @test nrequests == 1
        nrequests = test_status(:PUT, 400)
        @test nrequests == 1
    end

    @testset "403: Forbidden" begin
        # Returned when you pass an invalid api-key.
        # See https://www.rfc-editor.org/rfc/rfc9110#status.403
        nrequests = test_status(:GET, 403)
        @test nrequests == 1
        nrequests = test_status(:PUT, 403)
        @test nrequests == 1
    end

    @testset "404: Not Found" begin
        # Returned when container not found or blob not found
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.404
        nrequests = test_status(:GET, 404)
        @test nrequests == 1
    end

    @testset "405: Method Not Supported" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.405
        nrequests = test_status(:GET, 405, ["Allow" => "PUT"])
        @test nrequests == 1
        nrequests = test_status(:PUT, 405, ["Allow" => "GET"])
        @test nrequests == 1
    end

    @testset "409: Conflict" begin
        # Returned when write operations conflict.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.409
        nrequests = test_status(:GET, 409)
        @test nrequests == 1
        nrequests = test_status(:PUT, 409)
        @test nrequests == 1
    end

    @testset "412: Precondition Failed" begin
        # Returned when an If-Match or If-None-Match header's condition evaluates to false
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        # See https://www.rfc-editor.org/rfc/rfc9110#status.412
        nrequests = test_status(:GET, 412)
        @test nrequests == 1
        nrequests = test_status(:PUT, 412)
        @test nrequests == 1
    end

    @testset "413: Content Too Large" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.413
        nrequests = test_status(:PUT, 413)
        @test nrequests == 1
    end

    @testset "429: Too Many Requests" begin
        # See https://www.rfc-editor.org/rfc/rfc6585#section-4
        nrequests = test_status(:GET, 429)
        @test nrequests == 1
        nrequests = test_status(:PUT, 429)
        @test nrequests == 1
        # See https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
        # TODO: We probably should respect the Retry-After header, but we currently don't
        # (and we don't know if AWS actually sets it)
        # This can happen when AWS is throttling us, so it might be a good idea to retry with some
        # larger initial backoff (very eager retries probably only make the situation worse).
        nrequests = test_status(:GET, 429, ["Retry-After" => 10])
        @test nrequests == 1 + max_retries broken=true
        nrequests = test_status(:PUT, 429, ["Retry-After" => 10])
        @test nrequests == 1 + max_retries broken=true
    end

    @testset "502: Bad Gateway" begin
        # https://www.rfc-editor.org/rfc/rfc9110#status.502
        #   The 502 (Bad Gateway) status code indicates that the server, while acting as a
        #   gateway or proxy, received an invalid response from an inbound server it accessed
        #   while attempting to fulfill the request.
        # This error can occur when you enter HTTP instead of HTTPS in the connection.
        nrequests = test_status(:GET, 502)
        @test nrequests == 1 + max_retries
        nrequests = test_status(:PUT, 502)
        @test nrequests == 1 + max_retries
    end

    @testset "503: Service Unavailable" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.503
        #   The 503 (Service Unavailable) status code indicates that the server is currently
        #   unable to handle the request due to a temporary overload or scheduled maintenance,
        #   which will likely be alleviated after some delay. The server MAY send a Retry-After
        #   header field (Section 10.2.3) to suggest an appropriate amount of time for the
        #   client to wait before retrying the request.
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        nrequests = test_status(:GET, 503)
        @test nrequests == 1 + max_retries
        nrequests = test_status(:PUT, 503)
        @test nrequests == 1 + max_retries
    end

    @testset "504: Gateway Timeout" begin
        # See https://www.rfc-editor.org/rfc/rfc9110#status.504
        #   The 504 (Gateway Timeout) status code indicates that the server, while acting as
        #   a gateway or proxy, did not receive a timely response from an upstream server it
        #   needed to access in order to complete the request
        nrequests = test_status(:GET, 504)
        @test nrequests == 1 + max_retries
        nrequests = test_status(:PUT, 504)
        @test nrequests == 1 + max_retries
    end

    @testset "Timeout" begin
        nrequests = test_timeout(:GET, "timed out", 2)
        @test nrequests == 1 + max_retries
        nrequests = test_timeout(:PUT, "timed out", 2)
        @test nrequests == 1 + max_retries
    end

    @testset "TCP Closed" begin
        nrequests = test_tcp_error(:GET)
        @test nrequests == 1 + max_retries
        nrequests = test_tcp_error(:PUT)
        @test nrequests == 1 + max_retries
    end

    @testset "Incomplete GET body" begin
        nrequests = test_get_stream_error()
        @test nrequests == 1 + max_retries
    end
end
