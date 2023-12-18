@testitem "Basic BlobStorage exceptions" setup=[InitializeObjectStore] begin
    using CloudBase.CloudTest: Azurite
    import CloudBase
    using ObjectStore: blob_get!, blob_put, AzureCredentials
    import ObjectStore

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
                @test occursin("400 Bad Request", e.msg)
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

    @testset "multiple start" begin
        config = RustStoreConfig(5, 5)
        res = @ccall ObjectStore.rust_lib.start(config::RustStoreConfig)::Cint
        @test res == 1 # Rust CResult::Error
    end
end # @testitem
