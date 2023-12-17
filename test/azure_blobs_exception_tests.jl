@testitem "Basic BlobStorage exceptions" setup=[InitializeRustStore] begin
    using CloudBase.CloudTest: Azurite
    import CloudBase
    using ObjectStore: blob_get!, blob_put, AzureCredentials

    # For interactive testing, use Azurite.run() instead of Azurite.with()
    # conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
    Azurite.with(; debug=true, public=false) do conf
        _credentials, _container = conf
        base_url = _container.baseurl
        credentials = AzureCredentials(_credentials.auth.account, _container.name, _credentials.auth.key, base_url)
        global _stale_credentials = credentials
        global _stale_base_url = base_url

        @testset "Insufficient output buffer size" begin
            input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
            buffer = Vector{UInt8}(undef, 10)
            @assert sizeof(input) == 100
            @assert sizeof(buffer) < sizeof(input)

            nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
            @test nbytes_written == 100

            try
                nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
                @test false # Should have thrown an error
            catch err
                @test err isa ErrorException
                @test err.msg == "failed to process get with error: Supplied buffer was too small"
            end
        end

        @testset "Malformed credentials" begin
            input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
            buffer = Vector{UInt8}(undef, 100)
            bad_credentials = AzureCredentials(_credentials.auth.account, _container.name, "", base_url)

            try
                blob_put(joinpath(base_url, "invalid_credentials.csv"), codeunits(input), bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("400 Bad Request", e.msg) # Should this be 403 Forbidden? We've seen that with invalid SAS tokens
                @test occursin("Authentication information is not given in the correct format", e.msg)
            end

            nbytes_written = blob_put(joinpath(base_url, "invalid_credentials.csv"), codeunits(input), credentials)
            @assert nbytes_written == 100

            try
                blob_get!(joinpath(base_url, "invalid_credentials.csv"), buffer, bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("400 Bad Request", e.msg)
                @test occursin("Authentication information is not given in the correct format", e.msg)
            end
        end

        @testset "Non-existing file" begin
            buffer = Vector{UInt8}(undef, 100)
            try
                blob_get!(joinpath(base_url, "doesnt_exist.csv"), buffer, credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified blob does not exist", e.msg)
            end
        end

        @testset "Non-existing container" begin
            non_existent_container_name = string(credentials.container, "doesntexist")
            non_existent_base_url = replace(base_url, credentials.container => non_existent_container_name)
            bad_credentials = AzureCredentials(_credentials.auth.account, non_existent_container_name, credentials.key, non_existent_base_url)
            buffer = Vector{UInt8}(undef, 100)

            try
                blob_put(joinpath(base_url, "invalid_credentials2.csv"), codeunits("a,b,c"), bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified container does not exist", e.msg)
            end

            nbytes_written = blob_put(joinpath(base_url, "invalid_credentials2.csv"), codeunits("a,b,c"), credentials)
            @assert nbytes_written == 5

            try
                blob_get!(joinpath(base_url, "invalid_credentials2.csv"), buffer, bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified container does not exist", e.msg)
            end
        end

        @testset "Non-existing resource" begin
            bad_credentials = AzureCredentials("non_existing_account", credentials.container, credentials.key, base_url)
            buffer = Vector{UInt8}(undef, 100)

            try
                blob_put(joinpath(base_url, "invalid_credentials3.csv"), codeunits("a,b,c"), bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
                @test occursin("404 Not Found", e.msg)
                @test occursin("The specified resource does not exist.", e.msg)
            end

            nbytes_written = blob_put(joinpath(base_url, "invalid_credentials3.csv"), codeunits("a,b,c"), credentials)
            @assert nbytes_written == 5

            try
                blob_get!(joinpath(base_url, "invalid_credentials3.csv"), buffer, bad_credentials)
                @test false # Should have thrown an error
            catch e
                @test e isa ErrorException
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
            blob_put(joinpath(_stale_base_url, "still_doesnt_exist.csv"), codeunits("a,b,c"), _stale_credentials)
            @test false # Should have thrown an error
        catch e
            @test e isa ErrorException
            @test occursin("Connection refused", e.msg)
        end

        try
            blob_get!(joinpath(_stale_base_url, "still_doesnt_exist.csv"), buffer, _stale_credentials)
            @test false # Should have thrown an error
        catch e
            @test e isa ErrorException
            @test occursin("Connection refused", e.msg)
        end
    end
end # @testitem

@testitem "BlobStorage retries" setup=[InitializeRustStore] begin
    using CloudBase.CloudTest: Azurite
    import CloudBase
    using ObjectStore: blob_get!, blob_put, AzureCredentials
    import HTTP
    import Sockets

    max_retries = InitializeRustStore.max_retries

    function test_status(method, response_status, headers=nothing)
        @assert method === :GET || method === :PUT
        nretries = Ref(0)
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
            nretries[] += 1
            response = isnothing(headers) ? HTTP.Response(response_status, response_body) : HTTP.Response(response_status, headers, response_body)
            return response
        end

        baseurl = "http://127.0.0.1:$port/$account/$container/"
        creds = AzureCredentials(account, container, shared_key_from_azurite, baseurl)

        try
            method === :GET && blob_get!(joinpath(baseurl, "blob"), zeros(UInt8, 5), creds)
            method === :PUT && blob_put(joinpath(baseurl, "blob"), codeunits("a,b,c"), creds)
            @test false # Should have thrown an error
        catch e
            @test e isa ErrorException
            @test occursin(string(response_status), e.msg)
            response_status < 500 && (@test occursin("response body from the dummy server", e.msg))
        finally
            close(http_server)
        end
        wait(http_server)
        return nretries[]
    end

    # See https://learn.microsoft.com/en-us/rest/api/searchservice/http-status-codes

    @testset "400: Bad Request" begin
        # Returned when there's an error in the request URI, headers, or body. The response body
        # contains an error message explaining what the specific problem is.
        nretries = test_status(:GET, 400)
        @test nretries == 1 broken=true
        nretries = test_status(:PUT, 400)
        @test nretries == 1 broken=true
    end

    @testset "403: Forbidden" begin
        # Returned when you pass an invalid api-key.
        nretries = test_status(:GET, 403)
        @test nretries == 1 broken=true
        nretries = test_status(:PUT, 403)
        @test nretries == 1 broken=true
    end

    @testset "404: Not Found" begin
        nretries = test_status(:GET, 404)
        @test nretries == 1
    end

    @testset "405: Method Not Supported" begin
        nretries = test_status(:GET, 405, ["Allow" => "PUT"])
        @test nretries == 1 broken=true
        nretries = test_status(:PUT, 405, ["Allow" => "GET"])
        @test nretries == 1 broken=true
    end

    @testset "409: Conflict" begin
        # Returned when write operations conflict.
        # NOTE: We currently don't retry but maybe we should? This is probably a case where the
        # retry logic should add more noise to the backoff so that multiple writers don't collide on retry.
        nretries = test_status(:GET, 409)
        @test nretries == max_retries broken=true
        nretries = test_status(:PUT, 409)
        @test nretries == max_retries broken=true
    end

    @testset "412: Precondition Failed" begin
        # Returned when an If-Match or If-None-Match header's condition evaluates to false
        nretries = test_status(:GET, 412)
        @test nretries == 1
        nretries = test_status(:PUT, 412)
        @test nretries == 1
    end

    @testset "413: Content Too Large" begin
        # https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob?tabs=shared-access-signatures#remarks
        nretries = test_status(:PUT, 413)
        @test nretries == 1 broken=true
    end

    @testset "429: Too Many Requests" begin
        # TODO: We probably should respect the Retry-After header, but we currently don't
        # (and we don't know if Azure actually sets it)
        # NOTE: This can happen when Azure is throttling us, so it might be a good idea to retry with some
        # larger initial backoff (very eager retries probably only make the situation worse).
        nretries = test_status(:GET, 429, ["Retry-After" => 10])
        @test nretries == max_retries broken=true
        nretries = test_status(:PUT, 429, ["Retry-After" => 10])
        @test nretries == max_retries broken=true
    end

    @testset "502: Bad Gateway" begin
        # This error occurs when you enter HTTP instead of HTTPS in the connection.
        nretries = test_status(:GET, 502)
        @test nretries == 1 broken=true
        nretries = test_status(:PUT, 502)
        @test nretries == 1 broken=true
    end

    @testset "503: Service Unavailable" begin
        # NOTE: This seems similar to 429 and the Azure docs specifically say:
        #    Important: In this case, we highly recommend that your client code back off and wait before retrying
        nretries = test_status(:GET, 503)
        @test nretries == max_retries broken=true
        nretries = test_status(:PUT, 503)
        @test nretries == max_retries broken=true
    end

    @testset "504: Gateway Timeout" begin
        # Azure AI Search listens on HTTPS port 443. If your search service URL contains HTTP instead of HTTPS, a 504 status code is returned.
        nretries = test_status(:GET, 504)
        @test nretries == 1 broken=true
        nretries = test_status(:PUT, 504)
        @test nretries == 1 broken=true
    end
end
