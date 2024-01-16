@testitem "Basic AWS S3 usage" setup=[InitializeObjectStore] begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: get!, put, AwsConfig, ClientOptions

# For interactive testing, use Minio.run() instead of Minio.with()
# conf, p = Minio.run(; debug=true, public=false); atexit(() -> kill(p))
Minio.with(; debug=true, public=false) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    default_region = "us-east-1"
    config = AwsConfig(;
        region=default_region,
        bucket_name=_container.name,
        access_key_id=_credentials.access_key_id,
        secret_access_key=_credentials.secret_access_key,
        host=base_url
    )

    @testset "0B file, 0B buffer" begin
        buffer = Vector{UInt8}(undef, 0)

        nbytes_written = put(codeunits(""), "empty.csv", config)
        @test nbytes_written == 0

        nbytes_read = get!(buffer, "empty.csv", config)
        @test nbytes_read == 0
    end

    @testset "0B file, 1KB buffer" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = put(codeunits(""), "empty.csv", config)
        @test nbytes_written == 0

        nbytes_read = get!(buffer, "empty.csv", config)
        @test nbytes_read == 0
    end

    @testset "100B file, 100B buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 100

        nbytes_read = get!(buffer, "test100B.csv", config)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "100B file, 1KB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 1000)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) > sizeof(input)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 100

        nbytes_read = get!(buffer, "test100B.csv", config)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file, 1MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 1_000_000

        nbytes_read = get!(buffer, "test100B.csv", config)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # Large files should eventually use multipart upload / download requests
    @testset "20MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 20_000_000

        nbytes_read = get!(buffer, "test100B.csv", config)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file, 21MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 21_000_000)
        @assert sizeof(input) < sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 20_000_000

        nbytes_read = get!(buffer, "test100B.csv", config)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end
end # Azurite.with

end # @testitem

# NOTE: PUT on azure always requires credentials, while GET on public containers doesn't
@testitem "Basic AWS S3 usage (anonymous read enabled)" setup=[InitializeObjectStore] begin
# TODO: currently object_store defaults to Instance credentials when no other credentials are supplied
@test_skip begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: get!, put, AwsConfig, ClientOptions

# For interactive testing, use Minio.run() instead of Azurite.with()
# conf, p = Minio.run(; debug=true, public=true); atexit(() -> kill(p))
Minio.with(; debug=true, public=true) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    default_region = "us-east-1"
    config = AwsConfig(;
        region=default_region,
        bucket_name=_container.name,
        access_key_id=_credentials.access_key_id,
        secret_access_key=_credentials.secret_access_key,
        host=base_url
    )
    config_no_creds = AwsConfig(;
        region=default_region,
        bucket_name=_container.name,
        host=base_url
    )

    @testset "0B file, 0B buffer" begin
        buffer = Vector{UInt8}(undef, 0)

        nbytes_written = put(codeunits(""), "empty.csv", config)
        @test nbytes_written == 0

        nbytes_read = get!(buffer, "empty.csv", config_no_creds)
        @test nbytes_read == 0
    end

    @testset "0B file, 1KB buffer" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = put(codeunits(""), "empty.csv", config)
        @test nbytes_written == 0

        nbytes_read = get!(buffer, "empty.csv", config_no_creds)
        @test nbytes_read == 0
    end

    @testset "100B file, 100B buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 100

        nbytes_read = get!(buffer, "test100B.csv", config_no_creds)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "100B file, 1KB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 1000)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) > sizeof(input)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 100

        nbytes_read = get!(buffer, "test100B.csv", config_no_creds)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file, 1MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 1_000_000

        nbytes_read = get!(buffer, "test100B.csv", config_no_creds)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # Large files should eventually use multipart upload / vectored download requests
    @testset "20MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 20_000_000

        nbytes_read = get!(buffer, "test100B.csv", config_no_creds)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file, 21MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 21_000_000)
        @assert sizeof(input) < sizeof(buffer)

        nbytes_written = put(codeunits(input), "test100B.csv", config)
        @test nbytes_written == 20_000_000

        nbytes_read = get!(buffer, "test100B.csv", config_no_creds)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end
end # Azurite.with
end # @test_skip
end # @testitem
