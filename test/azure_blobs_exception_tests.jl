@testitem "Basic BlobStorage exceptions" setup=[InitializeObjectStore] begin
    using CloudBase.CloudTest: Azurite
    import CloudBase
    using RustyObjectStore: RustyObjectStore, get_object!, put_object, ClientOptions, AzureConfig, AWSConfig

    # For interactive testing, use Azurite.run() instead of Azurite.with()
    # conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
    Azurite.with(; debug=true, public=false) do conf
        _credentials, _container = conf
        base_url = _container.baseurl
        config = AzureConfig(;
            storage_account_name=_credentials.auth.account,
            container_name=_container.name,
            storage_account_key=_credentials.auth.key,
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
            bad_config = AzureConfig(;
                storage_account_name=_credentials.auth.account,
                container_name=_container.name,
                storage_account_key="",
                host=base_url
            )

            try
                put_object(codeunits(input), "invalid_credentials.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("400 Bad Request", e.msg) # Should this be 403 Forbidden? We've seen that with invalid SAS tokens
                @test occursin("Authentication information is not given in the correct format", e.msg)
            end

            nbytes_written = put_object(codeunits(input), "invalid_credentials.csv", config)
            @assert nbytes_written == 100

            try
                get_object!(buffer, "invalid_credentials.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("400 Bad Request", e.msg)
                @test occursin("Authentication information is not given in the correct format", e.msg)
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
                @test occursin("The specified blob does not exist", e.msg)
            end
        end

        @testset "Non-existing container" begin
            non_existent_container_name = string(_container.name, "doesntexist")
            non_existent_base_url = replace(base_url, _container.name => non_existent_container_name)
            bad_config = AzureConfig(;
                storage_account_name=_credentials.auth.account,
                container_name=non_existent_container_name,
                storage_account_key=_credentials.auth.key,
                host=non_existent_base_url
            )
            buffer = Vector{UInt8}(undef, 100)

            try
                put_object(codeunits("a,b,c"), "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified container does not exist", e.msg)
            end

            nbytes_written = put_object(codeunits("a,b,c"), "invalid_credentials2.csv", config)
            @assert nbytes_written == 5

            try
                get_object!(buffer, "invalid_credentials2.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified container does not exist", e.msg)
            end
        end

        @testset "Non-existing resource" begin
            bad_config = AzureConfig(;
                storage_account_name="non_existing_account",
                container_name=_container.name,
                storage_account_key=_credentials.auth.key,
                host=base_url
            )
            buffer = Vector{UInt8}(undef, 100)

            try
                put_object(codeunits("a,b,c"), "invalid_credentials3.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.PutException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified resource does not exist.", e.msg)
            end

            nbytes_written = put_object(codeunits("a,b,c"), "invalid_credentials3.csv", config)
            @assert nbytes_written == 5

            try
                get_object!(buffer, "invalid_credentials3.csv", bad_config)
                @test false # Should have thrown an error
            catch e
                @test e isa RustyObjectStore.GetException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified resource does not exist.", e.msg)
            end
        end
    end # Azurite.with
    # Azurite is not running at this point
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

### See Azure Blob Storage docs: https://learn.microsoft.com/en-us/rest/api/storageservices
### - "Common REST API error codes":
###   https://learn.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
### - "Azure Blob Storage error codes":
###   https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
### - "Get Blob"
###  https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob
### - "Put Blob"
###  https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
@testitem "BlobStorage retries" setup=[InitializeObjectStore] begin
    using CloudBase.CloudTest: Azurite
    import CloudBase
    using RustyObjectStore: get_object!, put_object, AzureConfig, ClientOptions
    import HTTP
    import Sockets

    max_retries = 2
    retry_timeout_secs = 2

    function test_status(method, response_status, headers=nothing)
        @assert method === :GET || method === :PUT
        nrequests = Ref(0)
        response_body = "response body from the dummy server"
        account = "myaccount"
        container = "mycontainer"
        shared_key_from_azurite = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

        (port, tcp_server) = Sockets.listenany(8081)
        http_server = HTTP.serve!(tcp_server) do request::HTTP.Request
            if request.method == "GET" && request.target == "/$account/$container/_this_file_does_not_exist"
                # This is the exploratory ping from connect_and_test in lib.rs
                return HTTP.Response(404, "Yup, still doesn't exist")
            end
            nrequests[] += 1
            response = isnothing(headers) ? HTTP.Response(response_status, response_body) : HTTP.Response(response_status, headers, response_body)
            return response
        end

        baseurl = "http://127.0.0.1:$port/$account/$container/"
        conf = AzureConfig(;
            storage_account_name=account,
            container_name=container,
            storage_account_key=shared_key_from_azurite,
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
            response_status < 500 && (@test occursin("response body from the dummy server", e.msg))
        finally
            close(http_server)
        end
        wait(http_server)
        return nrequests[]
    end

    @testset "400: Bad Request" begin
        # Returned when there's an error in the request URI, headers, or body. The response body
        # contains an error message explaining what the specific problem is.
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
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
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
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
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
        # See https://www.rfc-editor.org/rfc/rfc9110#status.409
        nrequests = test_status(:GET, 409)
        @test nrequests == 1
        nrequests = test_status(:PUT, 409)
        @test nrequests == 1
    end

    @testset "412: Precondition Failed" begin
        # Returned when an If-Match or If-None-Match header's condition evaluates to false
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob#blob-custom-properties
        # See https://www.rfc-editor.org/rfc/rfc9110#status.412
        nrequests = test_status(:GET, 412)
        @test nrequests == 1
        nrequests = test_status(:PUT, 412)
        @test nrequests == 1
    end

    @testset "413: Content Too Large" begin
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks
        #   If you attempt to upload either a block blob that's larger than the maximum
        #   permitted size for that service version or a page blob that's larger than 8 TiB,
        #   the service returns status code 413 (Request Entity Too Large). Blob Storage also
        #   returns additional information about the error in the response, including the
        #   maximum permitted blob size, in bytes.
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
        # (and we don't know if Azure actually sets it)
        # This can happen when Azure is throttling us, so it might be a good idea to retry with some
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
        # See https://learn.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
        #   An operation on any of the Azure Storage services can return the following error codes:
        #   Error code 	HTTP status code 	        User message
        #   ServerBusy 	Service Unavailable (503) 	The server is currently unable to receive requests. Please retry your request.
        #   ServerBusy 	Service Unavailable (503) 	Ingress is over the account limit.
        #   ServerBusy 	Service Unavailable (503) 	Egress is over the account limit.
        #   ServerBusy 	Service Unavailable (503) 	Operations per second is over the account limit.
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
end
