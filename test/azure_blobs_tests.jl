@testitem "Basic BlobStorage tests" begin

using CloudBase.CloudTest: Azurite
using ObjectStore: blob_get!, blob_put, AzureCredentials
import ObjectStore

ObjectStore.init_rust_store()

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true); atexit(() -> kill(p))
Azurite.with(; debug=true, public=false) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    credentials = AzureCredentials(_credentials.auth.account, _container.name, _credentials.auth.key, base_url)

    @testset "0B file" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = blob_put(joinpath(base_url, "empty.csv"), codeunits(""), credentials)
        @test nbytes_written == 0

        nbytes_read = blob_get!(joinpath(base_url, "empty.csv"), buffer, credentials)
        @test nbytes_read == 0
    end

    @testset "100B file" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 1000)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) > sizeof(input)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 100

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 1_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 20_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # If the buffer is too small, we hang
    # @testset "100B file, buffer too small" begin
    #     input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
    #     buffer = Vector{UInt8}(undef, 10)
    #     @assert sizeof(input) == 100
    #     @assert sizeof(buffer) < sizeof(input)

    #     nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
    #     @test nbytes_written == 100

    #     nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
    # end
end # Azurite.with

end # @testitem
