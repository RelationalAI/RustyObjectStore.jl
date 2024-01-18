module RustyObjectStore

export init_object_store, get_object!, put_object, StaticConfig, ClientOptions, Config, AzureConfig, AWSConfig
export ConfigBuilder, with_request_timeout_secs, with_connect_timeout_secs, with_max_retries, with_retry_timeout_secs,
       azure, with_container_name, with_storage_account_name, with_storage_account_key, with_storage_sas_token, build,
       aws, with_bucket_name, with_region, with_access_key, with_sts_token

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
end

function Base.show(io::IO, config::StaticConfig)
    print(io, "StaticConfig("),
    print(io, "n_threads=", Int(config.n_threads), ",")
    print(io, "cache_capacity=", Int(config.cache_capacity), ",")
    print(io, "cache_ttl_secs=", Int(config.cache_ttl_secs), ",")
    print(io, "cache_tti_secs=", Int(config.cache_tti_secs), ")")
end

const DEFAULT_CONFIG = StaticConfig(
    n_threads=0,
    cache_capacity=20,
    cache_ttl_secs=30 * 60,
    cache_tti_secs=5 * 60
)

const _OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()

struct InitException <: Exception
    msg::String
    return_code::Cint
end

"""
    init_object_store()
    init_object_store(config::StaticConfig)

Initialise object store.

This starts a `tokio` runtime for handling `object_store` requests.
It must be called before sending a request e.g. with `get_object!` or `put_object`.
The runtime is only started once and cannot be re-initialised with a different config,
subsequent `init_object_store` calls have no effect.

# Throws
- `InitException`: if the runtime fails to start.
"""
function init_object_store(config::StaticConfig=DEFAULT_CONFIG)
    @lock _INIT_LOCK begin
        if _OBJECT_STORE_STARTED[]
            return nothing
        end
        res = @ccall rust_lib.start(config::StaticConfig)::Cint
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
    host::Option{String}
    opts::ClientOptions
    cached_config::Config
    function AWSConfig(;
        region::String,
        bucket_name::String,
        access_key_id::Option{String} = nothing,
        secret_access_key::Option{String} = nothing,
        session_token::Option{String} = nothing,
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

        cached_config = Config("s3://$(bucket_name)/", params)
        return new(
            region,
            bucket_name,
            access_key_id,
            secret_access_key,
            session_token,
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
    @option_print(conf, host)
    print(io, ", ", "opts=", repr(conf.opts), ")")
end

struct Response
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}

    Response() = new(-1, 0, C_NULL)
end

abstract type RequestException <: Exception end
struct GetException <: RequestException
    msg::String
end
struct PutException <: RequestException
    msg::String
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
            @error "failed to submit get, internal channel closed"
            return 1
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
            @error "failed to submit put, internal channel closed"
            return 1
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

end # module
