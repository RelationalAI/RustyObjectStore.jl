@testsetup module ReadWriteCases
using RustyObjectStore: get_object!, put_object, get_object_stream, put_object_stream,
    AbstractConfig, delete_object, list_objects, list_objects_stream, next_chunk!, finish!
using CodecZlib
using RustyObjectStore

using Test: @testset, @test, @test_throws

export run_read_write_test_cases, run_stream_test_cases, run_sanity_test_cases, run_list_test_cases

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

    @testset "delete_object" begin
        input = "1,2,3,4,5,6,7,8,9,1\n" ^ 5
        buffer = Vector{UInt8}(undef, 100)
        @assert sizeof(input) == 100
        @assert sizeof(buffer) == sizeof(input)

        nbytes_written = put_object(codeunits(input), "test100B.csv", write_config)
        @test nbytes_written == 100

        delete_object("test100B.csv", write_config)

        try
            nbytes_read = get_object!(buffer, "test100B.csv", read_config)
            @test false # should throw
        catch e
            @test e isa RustyObjectStore.GetException
            @test occursin("not found", e.msg)
        end
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

function run_sanity_test_cases(read_config::AbstractConfig, write_config::AbstractConfig = read_config)
    @testset "Round trip" begin
        input = "1,2,3,4,5,6,7,8,9,1\n"
        buffer = Vector{UInt8}(undef, length(input))

        nbytes_written = put_object(codeunits(input), "roundtrip.csv", write_config)
        @test nbytes_written == length(input)

        nbytes_read = get_object!(buffer, "roundtrip.csv", read_config)
        @test nbytes_read == length(input)
        @test String(buffer[1:nbytes_read]) == input
    end
end

function within_margin(a, b, margin = 32)
    return all(abs.(a .- b) .<= margin)
end


function run_list_test_cases(config::AbstractConfig; strict_entry_size=true)
    margin = strict_entry_size ? 0 : 32
    @testset "basic listing" begin
        for i in range(10; step=10, length=5)
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        entries = list_objects("list/", config)
        @test length(entries) == 5
        @test within_margin(map(x -> x.size, entries), range(10; step=10, length=5), margin)
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

        entries = list_objects("other/", config)
        @test length(entries) == 10

        entries = list_objects("other/prefix/", config)
        @test length(entries) == 5
        @test within_margin(map(x -> x.size, entries), range(110; step=10, length=5), margin)
        @test map(x -> x.location, entries) ==
            ["other/prefix/110.csv", "other/prefix/120.csv", "other/prefix/130.csv", "other/prefix/140.csv", "other/prefix/150.csv"]

        entries = list_objects("other/nonexistent/", config)
        @test length(entries) == 0

        entries = list_objects("other/p/", config)
        @test length(entries) == 0

        entries = list_objects("other/prefix/150.csv", config)
        @test length(entries) == 1
        @test map(x -> x.location, entries) == ["other/prefix/150.csv"]
    end

    @testset "list empty entries" begin
        for i in range(10; step=10, length=3)
            nbytes_written = put_object(codeunits(""), "list_empty/$(i).csv", config)
            @test nbytes_written == 0
        end

        entries = list_objects("list_empty/", config)
        @test length(entries) == 3
        @test within_margin(sort(map(x -> x.size, entries)), [0, 0, 0], margin)
        @test map(x -> x.location, entries) == ["list_empty/10.csv", "list_empty/20.csv", "list_empty/30.csv"]
    end

    @testset "list stream" begin
        data = range(10; step=10, length=1001)
        for i in data
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        stream = list_objects_stream("list/", config)

        entries = next_chunk!(stream)
        @test length(entries) == max_entries_per_chunk()

        one_entry = next_chunk!(stream)
        @test length(one_entry) == 1

        @test isnothing(next_chunk!(stream))

        append!(entries, one_entry)

        @test within_margin(sort(map(x -> x.size, entries)), data, margin)
        @test sort(map(x -> x.location, entries)) == sort(map(x -> "list/$(x).csv", data))
    end

    @testset "list stream finish" begin
        data = range(10; step=10, length=1001)
        for i in data
            nbytes_written = put_object(codeunits(repeat('=', i)), "list/$(i).csv", config)
            @test nbytes_written == i
        end

        stream = list_objects_stream("list/", config)

        entries = next_chunk!(stream)
        @test length(entries) == max_entries_per_chunk()

        @test finish!(stream)

        @test isnothing(next_chunk!(stream))

        @test !finish!(stream)
    end

    @testset "list stream offset" begin
        key(x) = "offset/$(lpad(x, 10, "0")).csv"
        data = range(10; step=10, length=101)
        for i in data
            nbytes_written = put_object(codeunits(repeat('=', i)), key(i), config)
            @test nbytes_written == i
        end

        stream = list_objects_stream("offset/", config; offset=key(data[50]))

        entries = next_chunk!(stream)
        @test length(entries) == 51

        @test isnothing(next_chunk!(stream))

        @test within_margin(sort(map(x -> x.size, entries)), data[51:end], margin)
        @test sort(map(x -> x.location, entries)) == sort(map(x -> key(x), data[51:end]))
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
    run_list_test_cases(config)

    config_padded = AzureConfig(;
        storage_account_name=_credentials.auth.account * "  \n",
        container_name=_container.name * "  \n",
        storage_account_key=_credentials.auth.key * "  \n",
        host=base_url * "  \n"
    )

    run_sanity_test_cases(config_padded)
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
    run_list_test_cases(config)

    config_padded = AWSConfig(;
        region=default_region * " \n",
        bucket_name=_container.name * " \n",
        access_key_id=_credentials.access_key_id * " \n",
        secret_access_key=_credentials.secret_access_key * " \n",
        host=base_url * " \n"
    )

    run_sanity_test_cases(config_padded)
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

@testitem "Basic Snowflake Stage usage: AWS, non-encrypted" setup=[InitializeObjectStore, SnowflakeMock, ReadWriteCases] begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: SnowflakeConfig, ClientOptions

# For interactive testing, use Minio.run() instead of Minio.with()
# conf, p = Minio.run(; debug=true, public=false); atexit(() -> kill(p))
Minio.with(; debug=true, public=false) do conf
    credentials, container = conf
    with(SFGatewayMock(credentials, container, false)) do config::SnowflakeConfig
        run_read_write_test_cases(config)
        run_stream_test_cases(config)
        run_list_test_cases(config)
        run_sanity_test_cases(config)
    end
end # Minio.with
end # @testitem

@testitem "Basic Snowflake Stage usage: AWS, encrypted" setup=[InitializeObjectStore, SnowflakeMock, ReadWriteCases] begin
using CloudBase.CloudTest: Minio
using RustyObjectStore: SnowflakeConfig, ClientOptions

# For interactive testing, use Minio.run() instead of Minio.with()
# conf, p = Minio.run(; debug=true, public=false); atexit(() -> kill(p))
Minio.with(; debug=true, public=false) do conf
    credentials, container = conf
    with(SFGatewayMock(credentials, container, true)) do config::SnowflakeConfig
        run_read_write_test_cases(config)
        run_stream_test_cases(config)
        run_list_test_cases(config; strict_entry_size=false)
        run_sanity_test_cases(config)
    end
end # Minio.with
end # @testitem

@testitem "Basic Snowflake Stage usage: Azure, non-encrypted" setup=[InitializeObjectStore, SnowflakeMock, ReadWriteCases] begin
using CloudBase.CloudTest: Azurite
using RustyObjectStore: SnowflakeConfig, ClientOptions

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
Azurite.with(; debug=true, public=false) do conf
    credentials, container = conf
    with(SFGatewayMock(credentials, container, false)) do config::SnowflakeConfig
        run_read_write_test_cases(config)
        run_stream_test_cases(config)
        run_list_test_cases(config)
        run_sanity_test_cases(config)
    end
end # Azurite.with
end # @testitem

@testitem "Basic Snowflake Stage usage: Azure, encrypted" setup=[InitializeObjectStore, SnowflakeMock, ReadWriteCases] begin
using CloudBase.CloudTest: Azurite
using RustyObjectStore: SnowflakeConfig, ClientOptions

# For interactive testing, use Azurite.run() instead of Azurite.with()
# conf, p = Azurite.run(; debug=true, public=false); atexit(() -> kill(p))
Azurite.with(; debug=true, public=false) do conf
    credentials, container = conf
    with(SFGatewayMock(credentials, container, true)) do config::SnowflakeConfig
        run_read_write_test_cases(config)
        run_stream_test_cases(config)
        run_list_test_cases(config; strict_entry_size=false)
        run_sanity_test_cases(config)
    end
end # Azurite.with
end # @testitem
