@testitem "Basic BlobStorage usage" setup=[InitializeRustStore] begin
import CloudBase
using CloudBase.CloudTest: Azurite
using ObjectStore: blob_get!, blob_put, AzureConnection

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
Azurite.with(; debug=true, public=false) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    credentials = AzureConnection(_credentials.auth.account, _container.name;
                                  access_key = _credentials.auth.key,
                                  host = base_url)

    @testset "SAS token" begin
        token = CloudBase.generateAccountSASToken(_credentials.auth.account, _credentials.auth.key)
        sas_creds = AzureConnection(_credentials.auth.account, _container.name;
                                    sas_token = token,
                                    host = base_url)

        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = blob_put(joinpath(base_url, "sastest100B.csv"), codeunits(input), sas_creds)
        @test nbytes_written == 100

        nbytes_read = blob_get!(joinpath(base_url, "sastest100B.csv"), buffer, sas_creds)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "0B file, 0B buffer" begin
        buffer = Vector{UInt8}(undef, 0)

        nbytes_written = blob_put(joinpath(base_url, "empty.csv"), codeunits(""), credentials)
        @test nbytes_written == 0

        nbytes_read = blob_get!(joinpath(base_url, "empty.csv"), buffer, credentials)
        @test nbytes_read == 0
    end

    @testset "0B file, 1KB buffer" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = blob_put(joinpath(base_url, "empty.csv"), codeunits(""), credentials)
        @test nbytes_written == 0

        nbytes_read = blob_get!(joinpath(base_url, "empty.csv"), buffer, credentials)
        @test nbytes_read == 0
    end

    @testset "100B file, 100B buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 100

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "100B file, 1KB buffer" begin
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

    @testset "1MB file, 1MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 1_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # Large files should eventually use multipart upload / download requests
    @testset "20MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 20_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file, 21MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 21_000_000)
        @assert sizeof(input) < sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 20_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, credentials)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end
end # Azurite.with

end # @testitem

# NOTE: PUT on azure always requires credentials, while GET on public containers doesn't
@testitem "Basic BlobStorage usage (anonymous read enabled)" setup=[InitializeRustStore] begin
using CloudBase.CloudTest: Azurite
using ObjectStore: blob_get!, blob_put, AzureConnection

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true, public=true); atexit(() -> kill(p))
Azurite.with(; debug=true, public=true) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    credentials = AzureConnection(_credentials.auth.account, _container.name;
                                  access_key = _credentials.auth.key,
                                  host = base_url)
    anon_connection = AzureConnection(_credentials.auth.account, _container.name; host=base_url)

    @testset "0B file, 0B buffer" begin
        buffer = Vector{UInt8}(undef, 0)

        nbytes_written = blob_put(joinpath(base_url, "empty.csv"), codeunits(""), credentials)
        @test nbytes_written == 0

        nbytes_read = blob_get!(joinpath(base_url, "empty.csv"), buffer, anon_connection)
        @test nbytes_read == 0
    end

    @testset "0B file, 1KB buffer" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = blob_put(joinpath(base_url, "empty.csv"), codeunits(""), credentials)
        @test nbytes_written == 0

        nbytes_read = blob_get!(joinpath(base_url, "empty.csv"), buffer, anon_connection)
        @test nbytes_read == 0
    end

    @testset "100B file, 100B buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 100

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, anon_connection)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "100B file, 1KB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 1000)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) > sizeof(input)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 100

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, anon_connection)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file, 1MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 1_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, anon_connection)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # Large files should eventually use multipart upload / vectored download requests
    @testset "20MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 20_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, anon_connection)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file, 21MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 21_000_000)
        @assert sizeof(input) < sizeof(buffer)

        nbytes_written = blob_put(joinpath(base_url, "test100B.csv"), codeunits(input), credentials)
        @test nbytes_written == 20_000_000

        nbytes_read = blob_get!(joinpath(base_url, "test100B.csv"), buffer, anon_connection)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end
end # Azurite.with

end # @testitem
