module RustyObjectStore

export init_object_store, get_object!, put_object, StaticConfig, ClientOptions, Config, AzureConfig, AWSConfig
export status_code, is_connection, is_timeout, is_early_eof, is_unknown, is_parse_url
export get_object_stream, ReadStream, finish!

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using DocStringExtensions
using object_store_ffi_jll
using JSON3

const Option{T} = Union{T, Nothing}

const rust_lib = if haskey(ENV, "OBJECT_STORE_LIB")
    # For development, e.g. run `cargo build --release` and point to `target/release/` dir.
    # Note this is set a precompilation time, as `ccall` needs this to be a `const`,
    # so you need to restart Julia / recompile the package if you change it.
    lib_path = realpath(joinpath(ENV["OBJECT_STORE_LIB"], "libobject_store_ffi.$(dlext)"))
    @warn """
        Using unreleased object_store_ffi library:
            $(repr(contractuser(lib_path)))
        This is only intended for local development and should not be used in production.
        """
    lib_path
else
    object_store_ffi_jll.libobject_store_ffi
end

"""
    $TYPEDEF

Global configuration for the object store requests.

# Keywords
$TYPEDFIELDS
"""
@kwdef struct StaticConfig
    """
    The number of worker threads for the native pool,
    a value of zero makes it equal to the number of logical cores on the machine.
    """
    n_threads::Culonglong
    "The maximum capacity for the client cache"
    cache_capacity::Culonglong
    "The time-to-live in seconds for entries in the client cache"
    cache_ttl_secs::Culonglong
    "The time-to-idle in seconds for entries in the client cache"
    cache_tti_secs::Culonglong
    "Put requests with a size in bytes greater than this will use multipart operations"
    multipart_put_threshold::Culonglong
    "Get requests with a size in bytes greater than this will use multipart operations"
    multipart_get_threshold::Culonglong
    "The size in bytes for each part of multipart get operations"
    multipart_get_part_size::Culonglong
    "The max number of allowed Rust request tasks"
    concurrency_limit::Cuint
end

function Base.show(io::IO, config::StaticConfig)
    print(io, "StaticConfig("),
    print(io, "n_threads=", Int(config.n_threads), ",")
    print(io, "cache_capacity=", Int(config.cache_capacity), ",")
    print(io, "cache_ttl_secs=", Int(config.cache_ttl_secs), ",")
    print(io, "cache_tti_secs=", Int(config.cache_tti_secs), ",")
    print(io, "multipart_put_threshold=", Int(config.multipart_put_threshold), ",")
    print(io, "multipart_get_threshold=", Int(config.multipart_get_threshold), ",")
    print(io, "multipart_get_part_size=", Int(config.multipart_get_part_size), ")")
end

const DEFAULT_CONFIG = StaticConfig(
    n_threads=0,
    cache_capacity=20,
    cache_ttl_secs=30 * 60,
    cache_tti_secs=5 * 60,
    multipart_put_threshold=10 * 1024 * 1024,
    multipart_get_threshold=8 * 1024 * 1024,
    multipart_get_part_size=8 * 1024 * 1024,
    concurrency_limit=512
)

const _OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()

struct InitException <: Exception
    msg::String
    return_code::Cint
end

function default_panic_hook()
    println("Rust thread panicked, exiting the process")
    exit(1)
end

"""
    init_object_store()
    init_object_store(config::StaticConfig)
    init_object_store(config::StaticConfig; on_rust_panic::Function)

Initialise object store.

This starts a `tokio` runtime for handling `object_store` requests.
It must be called before sending a request e.g. with `get_object!` or `put_object`.
The runtime is only started once and cannot be re-initialised with a different config,
subsequent `init_object_store` calls have no effect.

An optional panic hook may be provided to react to panics on Rust's native threads.
The default behavior is to log and exit the process.

# Throws
- `InitException`: if the runtime fails to start.
"""
function init_object_store(
    config::StaticConfig=DEFAULT_CONFIG;
    on_rust_panic::Function=default_panic_hook
)
    @lock _INIT_LOCK begin
        if _OBJECT_STORE_STARTED[]
            return nothing
        end
        cond = Base.AsyncCondition()
        errormonitor(Threads.@spawn begin
            while true
                wait(cond)
                try
                    on_rust_panic()
                catch e
                    @error "Custom panic hook failed" exception=(e, catch_backtrace())
                end
            end
        end)
        panic_cond_handle = cond.handle
        res = @ccall rust_lib.start(config::StaticConfig, panic_cond_handle::Ptr{Cvoid})::Cint
        if res != 0
            throw(InitException("Failed to initialise object store runtime.", res))
        end
        _OBJECT_STORE_STARTED[] = true
    end
    return nothing
end

"""
    $TYPEDEF

# Keyword Arguments
- `request_timeout_secs::Option{Int}`: (Optional) Client request timeout in seconds.
- `connect_timeout_secs::Option{Int}`: (Optional) Client connection timeout in seconds.
- `max_retries::Option{Int}`: (Optional) Maximum number of retry attempts.
- `retry_timeout_secs::Option{Int}`: (Optional) Maximum amount of time from the initial request after which no further retries will be attempted (in seconds).
"""
struct ClientOptions
    params::Dict{String, String}

    function ClientOptions(;
        request_timeout_secs::Option{Int} = nothing,
        connect_timeout_secs::Option{Int} = nothing,
        max_retries::Option{Int} = nothing,
        retry_timeout_secs::Option{Int} = nothing
    )
        params = Dict()
        if !isnothing(request_timeout_secs)
            # Include `s` so parsing on Rust understands this as seconds
            params["timeout"] = string(request_timeout_secs, "s")
        end

        if !isnothing(connect_timeout_secs)
            # Include `s` so parsing on Rust understands this as seconds
            params["connect_timeout"] = string(connect_timeout_secs, "s")
        end

        if !isnothing(max_retries)
            params["max_retries"] = string(max_retries)
        end

        if !isnothing(retry_timeout_secs)
            # `s` suffix is not required as this field is already expected to be the number of seconds
            params["retry_timeout_secs"] = string(retry_timeout_secs)
        end

        return new(params)
    end
end

function Base.show(io::IO, opts::ClientOptions)
    dict = opts.params
    print(io, "ClientOptions(")
    parts = []
    haskey(dict, "timeout") &&
        push!(parts, string("request_timeout_secs=", parse(Int, rstrip(dict["timeout"], 's'))))
    haskey(dict, "connect_timeout") &&
        push!(parts, string("connect_timeout_secs=", parse(Int, rstrip(dict["connect_timeout"], 's'))))
    haskey(dict, "max_retries") &&
        push!(parts, string("max_retries=", parse(Int, dict["max_retries"])))
    haskey(dict, "retry_timeout_secs") &&
        push!(parts, string("retry_timeout_secs=", parse(Int, dict["retry_timeout_secs"])))

    print(io, join(parts, ", "))
    print(io, ")")
end

abstract type AbstractConfig end

"""
    $TYPEDEF

Opaque configuration type for dynamic configuration use cases.
This allows passing the url and configuration key-value pairs directly to the underlying library
for validation and dispatching.
It is recommended to reuse an instance for many operations.

# Arguments
- `url::String`: Url of the object store container root path.
  It must include the cloud specific url scheme (s3://, azure://, az://).
- `params::Dict{String, String}`: A set of key-value pairs to configure access to the object store.
  Refer to the object_store crate documentation for the list of all supported parameters.
"""
struct Config <: AbstractConfig
    # The serialized string is stored here instead of the constructor arguments
    # in order to avoid any serialization overhead when performing get/put operations.
    # For this to be effective the recommended usage pattern is to reuse this object often
    # instead of constructing for each use.
    config_string::String
    function Config(url::String, params::Dict{String, String})
        return new(url_params_to_config_string(url, params))
    end
end

function url_params_to_config_string(url::String, params::Dict{String, String})
    dict = merge(Dict("url" => url), params)
    return JSON3.write(dict)
end

into_config(conf::Config) = conf

function Base.show(io::IO, config::Config)
    dict = JSON3.read(config.config_string, Dict{String, String})
    print(io, "Config(")
    print(io, repr(dict["url"]), ", ")
    for key in keys(dict)
        if occursin(r"secret|token|key", string(key))
            dict[key] = "*****"
        end
    end
    print(io, repr(dict))
    print(io, ")")
end

const _ConfigFFI = Cstring

function Base.cconvert(::Type{Ref{Config}}, config::Config)
   config_ffi = Base.unsafe_convert(Cstring, Base.cconvert(Cstring, config.config_string))::_ConfigFFI
    # cconvert ensures its outputs are preserved during a ccall, so we can crate a pointer
    # safely in the unsafe_convert call.
    return config_ffi, Ref(config_ffi)
end
function Base.unsafe_convert(::Type{Ref{Config}}, x::Tuple{T,Ref{T}}) where {T<:_ConfigFFI}
    return Base.unsafe_convert(Ptr{_ConfigFFI}, x[2])
end

macro option_print(obj, name, hide = false)
    return esc(:( !isnothing($obj.$name)
        && print(io, ", ", $(string(name)), "=", $hide ? "*****" : repr($obj.$name)) ))
end


"""
    $TYPEDEF

Configuration for the Azure Blob object store backend.
Only one of `storage_account_key` or `storage_sas_token` is allowed for a given instance.

It is recommended to reuse an instance for many operations.

# Keyword Arguments
- `storage_account_name::String`: Azure storage account name.
- `container_name::String`: Azure container name.
- `storage_account_key::Option{String}`: (Optional) Azure storage account key (conflicts with storage_sas_token).
- `storage_sas_token::Option{String}`: (Optional) Azure storage SAS token (conflicts with storage_account_key).
- `host::Option{String}`: (Optional) Alternative Azure host. For example, if using Azurite.
- `opts::ClientOptions`: (Optional) Client configuration options.
"""
struct AzureConfig <: AbstractConfig
    storage_account_name::String
    container_name::String
    storage_account_key::Option{String}
    storage_sas_token::Option{String}
    host::Option{String}
    opts::ClientOptions
    cached_config::Config
    function AzureConfig(;
        storage_account_name::String,
        container_name::String,
        storage_account_key::Option{String} = nothing,
        storage_sas_token::Option{String} = nothing,
        host::Option{String} = nothing,
        opts::ClientOptions = ClientOptions()
    )
        if !isnothing(storage_account_key) && !isnothing(storage_sas_token)
            error("Should provide either a storage_account_key or a storage_sas_token")
        end

        params = copy(opts.params)

        params["azure_storage_account_name"] = storage_account_name
        params["azure_container_name"] = container_name

        if !isnothing(storage_account_key)
            params["azure_storage_account_key"] = storage_account_key
        elseif !isnothing(storage_sas_token)
            params["azure_storage_sas_token"] = storage_sas_token
        end

        if !isnothing(host)
            params["azurite_host"] = host
            params["azure_disable_emulator_key"] = "true"
        end

        if isnothing(storage_account_key) && isnothing(storage_sas_token)
            params["azure_skip_signature"] = "true"
        end

        cached_config = Config("az://$(container_name)/", params)
        return new(
            storage_account_name,
            container_name,
            storage_account_key,
            storage_sas_token,
            host,
            opts,
            cached_config
        )
    end
end

into_config(conf::AzureConfig) = conf.cached_config

function Base.show(io::IO, conf::AzureConfig)
    print(io, "AzureConfig("),
    print(io, "storage_account_name=", repr(conf.storage_account_name), ", ")
    print(io, "container_name=", repr(conf.container_name))
    @option_print(conf, storage_account_key, true)
    @option_print(conf, storage_sas_token, true)
    @option_print(conf, host)
    print(io, ", ", "opts=", repr(conf.opts), ")")
end

"""
    $TYPEDEF

Configuration for the AWS S3 object store backend.

It is recommended to reuse an instance for many operations.

# Keyword Arguments
- `region::String`: AWS S3 region.
- `bucket_name::String`: AWS S3 bucket name.
- `access_key_id::Option{String}`: (Optional) AWS S3 access key id.
- `secret_access_key::Option{String}`: (Optional) AWS S3 secret access key.
- `session_token::Option{String}`: (Optional) AWS S3 session_token.
- `host::Option{String}`: (Optional) Alternative S3 host. For example, if using Minio.
- `opts::ClientOptions`: (Optional) Client configuration options.
"""
struct AWSConfig <: AbstractConfig
    region::String
    bucket_name::String
    access_key_id::Option{String}
    secret_access_key::Option{String}
    session_token::Option{String}
    use_instance_metadata::Bool
    host::Option{String}
    opts::ClientOptions
    cached_config::Config
    function AWSConfig(;
        region::String,
        bucket_name::String,
        access_key_id::Option{String} = nothing,
        secret_access_key::Option{String} = nothing,
        session_token::Option{String} = nothing,
        use_instance_metadata::Bool = false,
        host::Option{String} = nothing,
        opts::ClientOptions = ClientOptions()
    )
        params = copy(opts.params)

        params["region"] = region
        params["bucket_name"] = bucket_name

        if !isnothing(access_key_id)
            params["aws_access_key_id"] = access_key_id
        end

        if !isnothing(secret_access_key)
            params["aws_secret_access_key"] = secret_access_key
        end

        if !isnothing(session_token)
            params["aws_session_token"] = session_token
        end

        if !isnothing(host)
            params["minio_host"] = host
        end

        if !use_instance_metadata && isnothing(access_key_id)
            params["aws_skip_signature"] = "true"
        end

        if use_instance_metadata && (!isnothing(access_key_id) || !isnothing(secret_access_key))
            error("Credentials should not be provided when using instance metadata")
        end

        cached_config = Config("s3://$(bucket_name)/", params)
        return new(
            region,
            bucket_name,
            access_key_id,
            secret_access_key,
            session_token,
            use_instance_metadata,
            host,
            opts,
            cached_config
        )
    end
end

into_config(conf::AWSConfig) = conf.cached_config

function Base.show(io::IO, conf::AWSConfig)
    print(io, "AWSConfig("),
    print(io, "region=", repr(conf.region), ", ")
    print(io, "bucket_name=", repr(conf.bucket_name))
    @option_print(conf, access_key_id, true)
    @option_print(conf, secret_access_key, true)
    @option_print(conf, session_token, true)
    conf.use_instance_metadata && print(io, "use_instance_metadata=", repr(conf.use_instance_metadata))
    @option_print(conf, host)
    print(io, ", ", "opts=", repr(conf.opts), ")")
end

struct Response
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}

    Response() = new(-1, 0, C_NULL)
end

abstract type ErrorReason end

struct ConnectionError <: ErrorReason end
struct StatusError <: ErrorReason
    code::Int
end
struct EarlyEOF <: ErrorReason end
struct TimeoutError <: ErrorReason end
struct ParseURLError <: ErrorReason end
struct UnknownError <: ErrorReason end

abstract type RequestException <: Exception end
struct GetException <: RequestException
    msg::String
    reason::ErrorReason

    GetException(msg) = new(msg, rust_message_to_reason(msg))
end
struct PutException <: RequestException
    msg::String
    reason::ErrorReason

    PutException(msg) = new(msg, rust_message_to_reason(msg))
end

function reason(e::GetException)
    return e.reason::ErrorReason
end

function reason(e::PutException)
    return e.reason::ErrorReason
end

function status_code(e::RequestException)
    return reason(e) isa StatusError ? reason(e).code : nothing
end

function is_connection(e::RequestException)
    return reason(e) isa ConnectionError
end

function is_timeout(e::RequestException)
    return reason(e) isa TimeoutError
end

function is_early_eof(e::RequestException)
    return reason(e) isa EarlyEOF
end

function is_parse_url(e::RequestException)
    return reason(e) isa ParseURLError
end

function is_unknown(e::RequestException)
    return reason(e) isa UnknownError
end


function rust_message_to_reason(msg::AbstractString)
    if contains(msg, "tcp connect error: deadline has elapsed") ||
        contains(msg, "tcp connect error: Connection refused")
        return ConnectionError()
    elseif contains(msg, "Client error with status")
        m = match(r"Client error with status (\d+) ", msg)
        if !isnothing(m)
            code = tryparse(Int, m.captures[1])
            if !isnothing(code)
                return StatusError(code)
            else
                return UnknownError()
            end
        else
            return UnknownError()
        end
    elseif contains(msg, "HTTP status server error")
        m = match(r"HTTP status server error \((\d+) ", msg)
        if !isnothing(m)
            code = tryparse(Int, m.captures[1])
            if !isnothing(code)
                return StatusError(code)
            else
                return UnknownError()
            end
        else
            return UnknownError()
        end
    elseif contains(msg, "connection closed before message completed") ||
        contains(msg, "end of file before message length reached")
        return EarlyEOF()
    elseif contains(msg, "timed out")
        return TimeoutError()
    elseif contains(msg, "Unable to convert URL") ||
        contains(msg, "Unable to recognise URL")
        return ParseURLError()
    else
        return UnknownError()
    end
end

"""
    get_object!(buffer, path, conf) -> Int

Send a get request to the object store.

Fetches the data bytes at `path` and writes them to the given `buffer`.

# Arguments
- `buffer::AbstractVector{UInt8}`: The buffer to write the object data to.
  The contents of the buffer will be mutated.
  The buffer must be at least as large as the data.
  The buffer will not be resized.
- `path::String`: The location of the data to fetch.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Returns
- `nbytes::Int`: The number of bytes read from the object store and written to the buffer.
  That is, `buffer[1:nbytes]` will contain the object data.

# Throws
- `GetException`: If the request fails for any reason, including if the `buffer` is too small.
"""
function get_object!(buffer::AbstractVector{UInt8}, path::String, conf::AbstractConfig)
    response_ref = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = into_config(conf)
    while true
        result = @ccall rust_lib.get(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            config::Ref{Config},
            response_ref::Ref{Response},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if result == 1
            throw(GetException("failed to submit get, internal channel closed"))
        elseif result == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response_ref[]
        if response.result == 1
            err = "failed to process get with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            throw(GetException(err))
        end

        return Int(response.length)
    end
end

"""
    put_object(buffer, path, conf) -> Int

Send a put request to the object store.

Atomically writes the data bytes in `buffer` to `path`.

# Arguments
- `buffer::AbstractVector{UInt8}`: The data to write to the object store.
  This buffer will not be mutated.
- `path::String`: The location to write data to.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Returns
- `nbytes::Int`: The number of bytes written to the object store.
  Is always equal to `length(buffer)`.

# Throws
- `PutException`: If the request fails for any reason.
"""
function put_object(buffer::AbstractVector{UInt8}, path::String, conf::AbstractConfig)
    response_ref = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = into_config(conf)
    while true
        result = @ccall rust_lib.put(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            config::Ref{Config},
            response_ref::Ref{Response},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if result == 1
            throw(PutException("failed to submit put, internal channel closed"))
        elseif result == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response_ref[]
        if response.result == 1
            err = "failed to process put with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            throw(PutException(err))
        end

        return Int(response.length)
    end
end

struct ReadResponseFFI
    result::Cint
    length::Culonglong
    eof::Cuchar
    error_message::Ptr{Cchar}

    ReadResponseFFI() = new(-1, 0, 0, C_NULL)
end

struct ReadStreamResponseFFI
    result::Cint
    stream::Ptr{Nothing}
    object_size::Culonglong
    error_message::Ptr{Cchar}

    ReadStreamResponseFFI() = new(-1, C_NULL, 0, C_NULL)
end

"""
    ReadStream


Opaque IO stream of object data.

It is necessary to `finish!` the stream if it is not run to completion.

"""
mutable struct ReadStream <: IO
    ptr::Ptr{Nothing}
    object_size::Int
    bytes_read::Int
    ended::Bool
    error::Option{String}
end

function Base.eof(io::ReadStream)
    if io.ended
        return true
    elseif bytesavailable(io) > 0
        return false
    else
        response_ref = Ref(ReadResponseFFI())
        cond = Base.AsyncCondition()
        cond_handle = cond.handle
        result = @ccall rust_lib.is_end_of_stream(
            io.ptr::Ptr{Cvoid},
            response_ref::Ref{ReadResponseFFI},
            cond_handle::Ptr{Cvoid}
        )::Cint

        @assert result == 0

        wait(cond)

        response = response_ref[]
        if response.result == 1
            err = "failed to process is_end_of_stream with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            io.error = err
            @ccall rust_lib.destroy_read_stream(io.ptr::Ptr{Nothing})::Cint
            throw(GetException(err))
        end

        eof = response.eof > 0

        if eof
            io.ended = true
        end

        return eof
    end
end
function Base.bytesavailable(io::ReadStream)
    if io.ended
        return 0
    else
        result = @ccall rust_lib.bytes_available(io.ptr::Ptr{Cvoid})::Clonglong
        @assert result >= 0
        return Int(result)
    end
end
function Base.close(io::ReadStream)
    finish!(io)
    return nothing
end

Base.isopen(io::ReadStream) = !io.ended && isnothing(io.error)
Base.iswritable(io::ReadStream) = false
Base.filesize(io::ReadStream) = io.object_size

function Base.readbytes!(io::ReadStream, dest::AbstractVector{UInt8}, n)
    eof(io) && return 0
    if n == typemax(Int)
        bytes_read = 0
        while !eof(io)
            bytes_to_read = 128 * 1024
            bytes_read + bytes_to_read > length(dest) && resize!(dest, bytes_read + bytes_to_read)
            bytes_read += GC.@preserve dest _unsafe_read(io, pointer(dest, bytes_read+1), bytes_to_read)
        end
        resize!(dest, bytes_read)
        return bytes_read
    else
        bytes_to_read = n == typemax(Int) ? 64 * 1024 : Int(n)
        bytes_to_read > length(dest) && resize!(dest, bytes_to_read)
        bytes_read = GC.@preserve dest _unsafe_read(io, pointer(dest), bytes_to_read)
        return bytes_read
    end
end

function Base.unsafe_read(io::ReadStream, p::Ptr{UInt8}, nb::UInt)
    if eof(io)
        nb > 0 && throw(EOFError())
        return nothing
    end
    bytes_read = _unsafe_read(io, p, Int(nb))
    eof(io) && nb > bytes_read && throw(EOFError())
    return nothing
end

# TranscodingStreams.jl are calling this method when Base.bytesavailable is zero
# to trigger buffer refill
function Base.read(io::ReadStream, ::Type{UInt8})
    eof(io) && throw(EOFError())
    buf = zeros(UInt8, 1)
    n = _unsafe_read(io, pointer(buf), 1)
    n < 1 && throw(EOFError())
    @inbounds b = buf[1]
    return b
end

function _forward(to::IO, from::IO)
    buf = Vector{UInt8}(undef, 64 * 1024)
    n = 0
    while !eof(from)
        bytes_read = readbytes!(from, buf, 64 * 1024)
        bytes_written = 0
        while bytes_written < bytes_read
            bytes_written += write(to, buf[bytes_written+1:bytes_read])
        end
        n += bytes_written
    end

    return n
end

function Base.write(to::IO, from::ReadStream)
    return _forward(to, from)
end

"""
    get_object_stream(path, conf; size_hint, decompress) -> ReadStream

Send a get request to the object store returning a stream of object data.

# Arguments
- `path::String`: The location of the data to fetch.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Keyword
- `size_hint::Int`: (Optional) Expected size of the object (optimization for small objects).
- `decompress::Option{String}`: (Optional) Compression algorithm to decode the response stream (supports gzip, deflate, zlib or zstd)

# Returns
- `stream::ReadStream`: The stream of object data chunks.

# Throws
- `GetException`: If the request fails for any reason.
"""
function get_object_stream(path::String, conf::AbstractConfig; size_hint::Int=0, decompress::String="")
    response_ref = Ref(ReadStreamResponseFFI())
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = into_config(conf)
    hint = convert(UInt64, size_hint)
    while true
        result = @ccall rust_lib.get_stream(
            path::Cstring,
            hint::Culonglong,
            decompress::Cstring,
            config::Ref{Config},
            response_ref::Ref{ReadStreamResponseFFI},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if result == 1
            throw(GetException("failed to submit get_stream, internal channel closed"))
        elseif result == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response_ref[]
        if response.result == 1
            err = "failed to process get_stream with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            # No need to destroy_read_stream in case of errors here
            throw(GetException(err))
        end

        return ReadStream(
            response.stream,
            convert(Int, response.object_size),
            0,
            false,
            nothing
        )
    end
end

function _unsafe_read(stream::ReadStream, dest::Ptr{UInt8}, bytes_to_read::Int)
    if stream.ended
        return nothing
    end
    if !isnothing(stream.error)
        @error "stream stopped by prevoius error: $(stream.error)"
        return nothing
    end

    response_ref = Ref(ReadResponseFFI())
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    result = @ccall rust_lib.read_from_stream(
        stream.ptr::Ptr{Cvoid},
        dest::Ptr{UInt8},
        bytes_to_read::Culonglong,
        bytes_to_read::Culonglong,
        response_ref::Ref{ReadResponseFFI},
        cond_handle::Ptr{Cvoid}
    )::Cint

    if result == 1
        @error "failed to submit read_from_stream, runtime not started"
        return nothing
    end

    wait(cond)

    response = response_ref[]
    if response.result == 1
        err = "failed to process read_from_stream with error: $(unsafe_string(response.error_message))"
        @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        stream.error = err
        @ccall rust_lib.destroy_read_stream(stream.ptr::Ptr{Nothing})::Cint
        throw(GetException(err))
    end

    if response.length > 0
        stream.bytes_read += response.length
        if response.eof == 0
            return convert(Int, response.length)
        else
            stream.ended = true
            return convert(Int, response.length)
        end
    else
        stream.ended = true
        return nothing
    end
end


"""
    finish!(stream::ReadStream) -> Bool

Finishes the stream reclaiming resources.

This function is not thread-safe.

# Arguments
- `stream::ReadStream`: The stream of object data.

# Returns
- `was_running::Bool`: Indicates if the stream was running when `finish!` was called.
"""
function finish!(stream::ReadStream)
    if stream.ended
        return false
    end
    if !isnothing(stream.error)
        return false
    end
    @ccall rust_lib.destroy_read_stream(stream.ptr::Ptr{Nothing})::Cint
    stream.ended = true
    return true
end

struct WriteResponseFFI
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}

    WriteResponseFFI() = new(-1, 0, C_NULL)
end

struct WriteStreamResponseFFI
    result::Cint
    stream::Ptr{Nothing}
    error_message::Ptr{Cchar}

    WriteStreamResponseFFI() = new(-1, C_NULL, C_NULL)
end

"""
    WriteStream


Opaque IO sink of object data.

It is necessary to call `shutdown!` to ensure data is persisted, or `cancel!` if the stream is to be discarded.

"""
mutable struct WriteStream <: IO
    ptr::Ptr{Nothing}
    bytes_written::Int
    destroyed::Bool
    error::Option{String}
end

"""
    put_object_stream(path, conf; compress) -> WriteStream

Send a put request to the object store returning a stream to write data into.

# Arguments
- `path::String`: The location where to write the object.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Keyword
- `compress::Option{String}`: (Optional) Compression algorithm to encode the stream (supports gzip, deflate, zlib or zstd)

# Returns
- `stream::WriteStream`: The stream where to write object data.

# Throws
- `PutException`: If the request fails for any reason.
"""
function put_object_stream(path::String, conf::AbstractConfig; compress::String="")
    response_ref = Ref(WriteStreamResponseFFI())
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = into_config(conf)
    while true
        result = @ccall rust_lib.put_stream(
            path::Cstring,
            compress::Cstring,
            config::Ref{Config},
            response_ref::Ref{WriteStreamResponseFFI},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if result == 1
            throw(PutException("failed to submit put_stream, internal channel closed"))
        elseif result == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response_ref[]
        if response.result == 1
            err = "failed to process put_stream with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            # No need to destroy_write_stream in case of errors here
            throw(PutException(err))
        end

        return WriteStream(
            response.stream,
            0,
            false,
            nothing
        )
    end
end

"""
    cancel!(stream::WriteStream) -> Bool

Cancels the stream reclaiming resources.

No partial writes will be observed.

This function is not thread-safe.

# Arguments
- `stream::WriteStream`: The writeable stream to be canceled.

# Returns
- `was_writeable::Bool`: Indicates if the stream was writeable when `cancel!` was called.
"""
function cancel!(stream::WriteStream)
    if stream.destroyed
        return false
    end
    if !isnothing(stream.error)
        return false
    end
    @ccall rust_lib.destroy_write_stream(stream.ptr::Ptr{Nothing})::Cint
    stream.destroyed = true
    return true
end

"""
    shutdown!(stream::WriteStream) -> Bool

Shuts down the stream ensuring the data is persisted.

On failure partial writes will NOT be observed.

This function is not thread-safe.

# Arguments
- `stream::WriteStream`: The writeable stream to be shutdown.
"""
function shutdown!(stream::WriteStream)
    if !isnothing(stream.error)
        throw(PutException("Tried to shutdown a stream in error state, previous error: $(stream.error)"))
    end
    if stream.destroyed
        throw(PutException("Tried to shutdown a destroyed stream (from a previous `cancel!` or `shutdown!`)"))
    end

    response_ref = Ref(WriteResponseFFI())
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    result = @ccall rust_lib.shutdown_write_stream(
        stream.ptr::Ptr{Cvoid},
        response_ref::Ref{WriteResponseFFI},
        cond_handle::Ptr{Cvoid}
    )::Cint

    if result == 1
        throw(PutException("failed to submit shutdown_write_stream, runtime not started"))
    end

    wait(cond)

    response = response_ref[]
    if response.result == 1
        err = "failed to process shutdown_write_stream with error: $(unsafe_string(response.error_message))"
        @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        stream.error = err
        @ccall rust_lib.destroy_write_stream(stream.ptr::Ptr{Nothing})::Cint
        stream.destroyed = true
        throw(PutException(err))
    elseif response.result == 0
        @ccall rust_lib.destroy_write_stream(stream.ptr::Ptr{Nothing})::Cint
        stream.destroyed = true
        return nothing
    else
        @assert false "unreachable"
    end
end

Base.isopen(io::WriteStream) = !io.destroyed && isnothing(io.error)
Base.iswritable(io::WriteStream) = true
function Base.close(io::WriteStream)
    shutdown!(io)
    return nothing
end
function Base.flush(stream::WriteStream)
    _unsafe_write(stream, convert(Ptr{UInt8}, C_NULL), 0; flush=true)
    return nothing
end
function Base.unsafe_write(stream::WriteStream, input::Ptr{UInt8}, nbytes::Int)
    _unsafe_write(stream, input, nbytes)
    return nothing
end
function Base.write(io::WriteStream, bytes::Vector{UInt8})
    return _unsafe_write(io, pointer(bytes), length(bytes))
end
function Base.write(to::WriteStream, from::IO)
    return _forward(to, from)
end
function Base.write(to::WriteStream, from::ReadStream)
    return _forward(to, from)
end

function _unsafe_write(stream::WriteStream, input::Ptr{UInt8}, nbytes::Int; flush=false)
    if !isnothing(stream.error)
        throw(PutException("Tried to write to a stream in error state, previous error: $(stream.error)"))
    end
    if stream.destroyed
        throw(PutException("Tried to write to a destroyed stream (from a previous `cancel!` or `shutdown!`)"))
    end

    response_ref = Ref(WriteResponseFFI())
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    result = @ccall rust_lib.write_to_stream(
        stream.ptr::Ptr{Cvoid},
        input::Ptr{UInt8},
        nbytes::Culonglong,
        flush::Cuchar,
        response_ref::Ref{WriteResponseFFI},
        cond_handle::Ptr{Cvoid}
    )::Cint

    if result == 1
        throw(PutException("failed to submit write_to_stream, runtime not started"))
    end

    wait(cond)

    response = response_ref[]
    if response.result == 1
        err = "failed to process write_to_stream with error: $(unsafe_string(response.error_message))"
        @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        stream.error = err
        @ccall rust_lib.destroy_write_stream(stream.ptr::Ptr{Nothing})::Cint
        stream.destroyed = true
        throw(PutException(err))
    end

    @assert response.result == 0

    stream.bytes_written += response.length
    return Int(response.length)
end

end # module
