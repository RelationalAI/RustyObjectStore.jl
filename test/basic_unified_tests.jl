@testsetup module ReadWriteCases
using RustyObjectStore: get_object!, put_object, list_objects, list_objects_stream, next_chunk!, finish!, AbstractConfig

using Test: @testset, @test, @test_skip

export run_read_write_test_cases, run_list_test_cases

function run_read_write_test_cases(read_config::AbstractConfig, write_config::AbstractConfig = read_config)
    @testset "0B file, 0B buffer" begin
        buffer = Vector{UInt8}(undef, 0)

        nbytes_written = put_object(codeunits(""), "empty.csv", write_config)
        @test nbytes_written == 0

        nbytes_read = get_object!(buffer, "empty.csv", read_config)
        @test nbytes_read == 0
    end

    @testset "0B file, 1KB buffer" begin
        buffer = Vector{UInt8}(undef, 1000)

        nbytes_written = put_object(codeunits(""), "empty.csv", write_config)
        @test nbytes_written == 0

        nbytes_read = get_object!(buffer, "empty.csv", read_config)
        @test nbytes_read == 0
    end

    @testset "100B file, 100B buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 100

        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "100B file, 1KB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 1000)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) > sizeof(input)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 100

        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 100
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file, 1MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000
        buffer = Vector{UInt8}(undef, 1_000_000)
        @assert sizeof(input) == 1_000_000 == sizeof(buffer)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 1_000_000

        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    # Large files should use multipart upload / download requests
    @testset "20MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 20_000_000)
        @assert sizeof(input) == 20_000_000 == sizeof(buffer)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 20_000_000

        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "20MB file, 21MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 1_000_000
        buffer = Vector{UInt8}(undef, 21_000_000)
        @assert sizeof(input) < sizeof(buffer)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 20_000_000

        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 20_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

    @testset "1MB file, 20MB buffer" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 50_000

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 1_000_000

        # Edge case for multpart download, file is less than threshold but buffer is greater
        buffer = Vector{UInt8}(undef, 20_000_000)
        nbytes_read = get_object!(buffer, "test100B.csv", read_config)
        @test nbytes_read == 1_000_000
        @test String(buffer[1:nbytes_read]) == input
    end

end

function run_list_test_cases(config::AbstractConfig)
    @testset "basic listing" begin
        for i in range(10; step=10, length=5)
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        entries = list_objects("list", config)
        @test length(entries) == 5
        @test map(x -> x.size, entries) == range(10; step=10, length=5)
        @test map(x -> x.location, entries) == ["list/10.csv", "list/20.csv", "list/30.csv", "list/40.csv", "list/50.csv"]
    end

    @testset "basic prefix" begin
        for i in range(10; step=10, length=5)
            nbytes_written = put_object(codeunits(repeat('=', i)), "other/$(i).csv", config)
            @test nbytes_written == i
        end

        for i in range(110; step=10, length=5)
            nbytes_written = put_object(codeunits(repeat('=', i)), "other/prefix/$(i).csv", config)
            @test nbytes_written == i
        end

        entries = list_objects("other", config)
        @test length(entries) == 10

        entries = list_objects("other/prefix", config)
        @test length(entries) == 5
        @test map(x -> x.size, entries) == range(110; step=10, length=5)
        @test map(x -> x.location, entries) ==
            ["other/prefix/110.csv", "other/prefix/120.csv", "other/prefix/130.csv", "other/prefix/140.csv", "other/prefix/150.csv"]

        entries = list_objects("other/nonexistent", config)
        @test length(entries) == 0

        entries = list_objects("other/p", config)
        @test length(entries) == 0
    end

    # FIXME this test does not work on Azure Blob Storage due to a workaround on the object_store library
    # that skips empty blobs on the response. (see: https://github.com/apache/arrow-rs/blob/cf8084940d7b41d3f3d066a993981b20bb006e0c/object_store/src/azure/client.rs#L573)
    @testset "list empty entries" begin
        for i in range(10; step=10, length=3)
            nbytes_written = put_object(codeunits(""), "list_empty/$(i).csv", config)
            @test nbytes_written == 0
        end

        entries = list_objects("list_empty", config)
        @test length(entries) == 3 skip=true
        @test map(x -> x.size, entries) == [0, 0, 0] skip=true
        @test map(x -> x.location, entries) == ["list_empty/10.csv", "list_empty/20.csv", "list_empty/30.csv"] skip=true
    end

    @testset "list stream" begin
        data = range(10; step=10, length=1001)
        for i in data
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        stream = list_objects_stream("list", config)

        entries = next_chunk!(stream)
        @test length(entries) == 1000

        one_entry = next_chunk!(stream)
        @test length(one_entry) == 1

        @test isnothing(next_chunk!(stream))

        append!(entries, one_entry)

        @test sort(map(x -> x.size, entries)) == data
        @test sort(map(x -> x.location, entries)) == sort(map(x -> "list/$(x).csv", data))
    end

    @testset "list stream finish" begin
        data = range(10; step=10, length=1001)
        for i in data
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        stream = list_objects_stream("list", config)

        entries = next_chunk!(stream)
        @test length(entries) == 1000

        @test finish!(stream)

        @test isnothing(next_chunk!(stream))

        @test !finish!(stream)
    end
end
end # @testsetup

@testitem "Basic BlobStorage usage" setup=[InitializeObjectStore, ReadWriteCases] begin
using CloudBase.CloudTest: Azurite
using RustyObjectStore: AzureConfig, ClientOptions


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

    run_read_write_test_cases(config)
    run_list_test_cases(config)
end # Azurite.with

end # @testitem

# NOTE: PUT on azure always requires credentials, while GET on public containers doesn't
@testitem "Basic BlobStorage usage (anonymous read enabled)" setup=[InitializeObjectStore, ReadWriteCases] begin
using CloudBase.CloudTest: Azurite
using RustyObjectStore: AzureConfig, ClientOptions

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true, public=true); atexit(() -> kill(p))
Azurite.with(; debug=true, public=true) do conf
    _credentials, _container = conf
    base_url = _container.baseurl
    config = AzureConfig(;
        storage_account_name=_credentials.auth.account,
        container_name=_container.name,
        storage_account_key=_credentials.auth.key,
        host=base_url
    )
    config_no_creds = AzureConfig(;
        storage_account_name=_credentials.auth.account,
        container_name=_container.name,
        host=base_url
    )

    run_read_write_test_cases(config_no_creds, config)
end # Azurite.with
end # @testitem

@testitem "Basic AWS S3 usage" setup=[InitializeObjectStore, ReadWriteCases] begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: AWSConfig, ClientOptions

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

    run_read_write_test_cases(config)
    run_list_test_cases(config)
end # Minio.with
end # @testitem

@testitem "Basic AWS S3 usage (anonymous read enabled)" setup=[InitializeObjectStore, ReadWriteCases] begin
# TODO: currently object_store defaults to Instance credentials when no other credentials are supplied
# (see https://github.com/RelationalAI/RustyObjectStore.jl/issues/22)
@test_skip begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: AWSConfig, ClientOptions

# For interactive testing, use Minio.run() instead of Azurite.with()
# conf, p = Minio.run(; debug=true, public=true); atexit(() -> kill(p))
Minio.with(; debug=true, public=true) do conf
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
    config_no_creds = AWSConfig(;
        region=default_region,
        bucket_name=_container.name,
        host=base_url
    )

    run_read_write_test_cases(config_no_creds, config)
end # Minio.with
end # @test_skip
end # @testitem
