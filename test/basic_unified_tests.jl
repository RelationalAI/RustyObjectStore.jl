@testsetup module ReadWriteCases
using RustyObjectStore: get_object!, put_object, get_object_stream, put_object_stream, AbstractConfig
using CodecZlib
using RustyObjectStore

using Test: @testset, @test, @test_throws

export run_read_write_test_cases, run_stream_test_cases

function run_stream_test_cases(config::AbstractConfig)
    # ReadStream
    @testset "ReadStream small readbytes!" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^10; # 200 B
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 200

        buffer = Vector{UInt8}(undef, 200)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 200

        N = 19
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, buffer, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = get_object_stream("test.csv", config)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream large readbytes!" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 20 * 1000 * 1000

        buffer = Vector{UInt8}(undef, 20 * 1000 * 1000)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 20 * 1000 * 1000

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, buffer, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = get_object_stream("test.csv", config)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream small unsafe_read" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^10; # 200 B
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 200

        buffer = Vector{UInt8}(undef, 200)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 200

        N = 19
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, buffer, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = get_object_stream("test.csv", config)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            unsafe_read(ioobj, pointer(buf), nb)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream large unsafe_read" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 20 * 1000 * 1000

        buffer = Vector{UInt8}(undef, 20 * 1000 * 1000)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 20 * 1000 * 1000

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, buffer, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = get_object_stream("test.csv", config)
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            unsafe_read(ioobj, pointer(buf), nb)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream small readbytes! decompress" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^100; # 2000 B
        codec = ZlibCompressor()
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        nbytes_written = put_object(compressed, "test.csv.gz", config)
        @test nbytes_written == length(compressed)
        CodecZlib.finalize(codec)

        buffer = Vector{UInt8}(undef, length(compressed))
        nbytes_read = get_object!(buffer, "test.csv.gz", config)
        @test nbytes_read == length(compressed)

        N = 19
        buf = Vector{UInt8}(undef, N)

        ioobj = get_object_stream("test.csv.gz", config; decompress="zlib")
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream large readbytes! decompress" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        codec = ZlibCompressor()
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        nbytes_written = put_object(compressed, "test.csv.gz", config)
        @test nbytes_written == length(compressed)
        CodecZlib.finalize(codec)

        buffer = Vector{UInt8}(undef, length(compressed))
        nbytes_read = get_object!(buffer, "test.csv.gz", config)
        @test nbytes_read == length(compressed)

        N = 1024*1024
        buf = Vector{UInt8}(undef, N)

        ioobj = get_object_stream("test.csv.gz", config; decompress="zlib")
        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv) - i : N
            readbytes!(ioobj, buf, N)
            @test view(buf, 1:nb) == view(codeunits(multicsv), i:i+nb-1)
            i += N
        end

        close(ioobj)
    end
    @testset "ReadStream empty file readbytes! decompress" begin
        multicsv = "" # 0 MB
        codec = ZlibCompressor()
        CodecZlib.initialize(codec)
        compressed = transcode(codec, codeunits(multicsv))
        nbytes_written = put_object(compressed, "test.csv.gz", config)
        @test nbytes_written == length(compressed)
        CodecZlib.finalize(codec)

        buffer = Vector{UInt8}(undef, length(compressed))
        nbytes_read = get_object!(buffer, "test.csv.gz", config)
        @test nbytes_read == length(compressed)

        N = 1024*1024
        buf = ones(UInt8, N)

        ioobj = get_object_stream("test.csv.gz", config; decompress="zlib")
        readbytes!(ioobj, buf, N)
        @test eof(ioobj)
        @test all(buf .== 1)

        close(ioobj)
    end
    @testset "ReadStream empty file readbytes!" begin
        multicsv = "" # 0 MB
        data = codeunits(multicsv)
        nbytes_written = put_object(data, "test.csv", config)
        @test nbytes_written == length(data)

        buffer = Vector{UInt8}(undef, length(data))
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == length(data)

        N = 1024*1024
        buf = ones(UInt8, N)

        ioobj = get_object_stream("test.csv", config)
        readbytes!(ioobj, buf, N)
        @test eof(ioobj)
        @test all(buf .== 1)

        close(ioobj)
    end
    @testset "ReadStream empty file unsafe_read" begin
        multicsv = "" # 0 MB
        data = codeunits(multicsv)
        nbytes_written = put_object(data, "test.csv", config)
        @test nbytes_written == length(data)

        buffer = Vector{UInt8}(undef, length(data))
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == length(data)

        N = 1024*1024
        buf = ones(UInt8, N)

        ioobj = get_object_stream("test.csv", config)
        @test_throws EOFError unsafe_read(ioobj, pointer(buf), N)
        @test eof(ioobj)
        @test all(buf .== 1)

        close(ioobj)
    end
    @testset "ReadStream read last byte" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 20 * 1000 * 1000

        buffer = Vector{UInt8}(undef, 20 * 1000 * 1000)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 20 * 1000 * 1000

        N = length(multicsv) - 1
        buf = Vector{UInt8}(undef, N)
        copyto!(buf, 1, buffer, 1, N)
        @test buf == view(codeunits(multicsv), 1:N)

        ioobj = get_object_stream("test.csv", config)
        readbytes!(ioobj, buf, N)
        @test buf == view(codeunits(multicsv), 1:N)
        @test read(ioobj, UInt8) == UInt8(last(multicsv))

        close(ioobj)
    end
    @testset "ReadStream read bytes into file" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20 MB
        nbytes_written = put_object(codeunits(multicsv), "test.csv", config)
        @test nbytes_written == 20 * 1000 * 1000

        buffer = Vector{UInt8}(undef, 20 * 1000 * 1000)
        nbytes_read = get_object!(buffer, "test.csv", config)
        @test nbytes_read == 20 * 1000 * 1000


        (path, io) = mktemp()
        rs = get_object_stream("test.csv", config)
        write(io, rs)
        close(io)

        io = open(path, "r")
        filedata = read(io)
        @test length(filedata) == length(codeunits(multicsv))
        close(io)

        @test buffer == codeunits(multicsv)

        close(rs)
    end

    # WriteStream
    @testset "WriteStream write small bytes" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^100; # 2000 B

        N = 2000
        ws = put_object_stream("test.csv", config)

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            write(ws, buf)
            i += N
        end

        close(ws)

        rs = get_object_stream("test.csv", config)
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
    @testset "WriteStream write large bytes" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 2000000
        ws = put_object_stream("test.csv", config)

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            write(ws, buf)
            i += N
        end

        close(ws)

        rs = get_object_stream("test.csv", config)
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
    @testset "WriteStream write empty" begin
        multicsv = ""; # 0 B

        ws = put_object_stream("test.csv", config)

        write(ws, codeunits(multicsv))

        close(ws)

        rs = get_object_stream("test.csv", config)
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
    @testset "WriteStream write small bytes and compress" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^100; # 2000 B

        N = 2000
        ws = put_object_stream("test.csv.gz", config; compress="gzip")

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            write(ws, buf)
            i += N
        end

        close(ws)

        rs = get_object_stream("test.csv.gz", config; decompress="gzip")
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
    @testset "WriteStream write large bytes and compress" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 2000000
        ws = put_object_stream("test.csv", config; compress="gzip")

        i = 1
        while i < sizeof(multicsv)
            nb = i + N > length(multicsv) ? length(multicsv)-i+1 : N
            buf = Vector{UInt8}(undef, nb)
            copyto!(buf, 1, codeunits(multicsv), i, nb)
            write(ws, buf)
            i += N
        end

        close(ws)

        rs = get_object_stream("test.csv", config; decompress="gzip")
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
    @testset "WriteStream write bytes from file" begin
        multicsv = "1,2,3,4,5,6,7,8,9,1\n"^1000000; # 20MB

        N = 2000000

        (path, io) = mktemp()
        written = write(io, codeunits(multicsv))
        @test written == length(codeunits(multicsv))
        close(io)

        ws = put_object_stream("test.csv", config)

        io = open(path, "r")
        write(ws, io)
        close(ws)

        rs = get_object_stream("test.csv", config)
        objdata = read(rs)
        @test objdata == codeunits(multicsv)
    end
end

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
    run_stream_test_cases(config)
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
    run_stream_test_cases(config)

end # Minio.with
end # @testitem

@testitem "Basic AWS S3 usage (anonymous read enabled)" setup=[InitializeObjectStore, ReadWriteCases] begin
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
end # @testitem
