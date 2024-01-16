module RustyObjectStore

export init_object_store, get!, put, ClientOptions, Config, AzureConfig, AwsConfig
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

const _OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()

struct InitException <: Exception
    msg::String
    return_code::Cint
end

"""
    init_object_store()

Initialise object store.

This starts a `tokio` runtime for handling `object_store` requests.
It must be called before sending a request e.g. with `blob_get!` or `blob_put`.
The runtime is only started once and cannot be re-initialised,
subsequent `init_object_store` calls have no effect.

# Throws
- `InitException`: if the runtime fails to start.
"""
function init_object_store()
    @lock _INIT_LOCK begin
        if _OBJECT_STORE_STARTED[]
            return nothing
        end
        res = @ccall rust_lib.start()::Cint
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
- `request_timeout_secs::Int`: (Optional) Client request timeout in seconds.
- `connect_timeout_secs::Int`: (Optional) Client connection timeout in seconds.
- `max_retries::Int`: (Optional) Maximum number of retry attempts.
- `retry_timeout_secs::Int`: (Optional) Maximum amount of time from the initial request after which no further retries will be attempted (in seconds).
"""
struct ClientOptions
    params::Dict{String, String}

    function ClientOptions(;
        request_timeout_secs::Int = 30,
        connect_timeout_secs::Int = 5,
        max_retries::Int = 10,
        retry_timeout_secs::Int = 150
    )
        params = Dict(
            "timeout" => "$(string(request_timeout_secs))s",
            "connect_timeout" => "$(string(connect_timeout_secs))s",
            "max_retries" => string(max_retries),
            "retry_timeout_secs" => string(retry_timeout_secs)
        )

        return new(params)
    end
end

abstract type AsConfig end

function as_config(wrapper::AsConfig) end

"""
    $TYPEDEF

# Arguments
- `url::String`: Url of the object store container root path.
  It must include the cloud specific url scheme (s3://, azure:// ...).
- `params::Dict{String, String}`: A set of key-value pairs to configure access to the object store.
  Refer to the object_store crate documentation for the list of all supported parameters.
"""
struct Config <: AsConfig
    config_string::String
    function Config(url::String, params::Dict{String, String})
        dict = merge(Dict("url" => url), params)
        config_string = JSON3.write(dict)
        new(config_string)
    end
end

as_config(wrapper::Config) = wrapper

"""
    $TYPEDEF

# Keyword Arguments
- `storage_account_name::String`: Azure storage account name.
- `container_name::String`: Azure container name.
- `storage_account_key::Option{String}`: (Optional) Azure storage account key.
- `storage_sas_token::Option{String}`: (Optional) Azure storage SAS token.
- `host::Option{String}`: (Optional) Alternative Azure host. For example, if using Azurite.
- `opts::ClientOptions`: (Optional) Client configuration options.
"""
struct AzureConfig <: AsConfig
    config::Config
    function AzureConfig(;
        storage_account_name::String,
        container_name::String,
        storage_account_key::Option{String} = nothing,
        storage_sas_token::Option{String} = nothing,
        host::Option{String} = nothing,
        opts::ClientOptions = ClientOptions()
    )
        (
            !isnothing(storage_account_key)
            && !isnothing(storage_sas_token)
        ) && error("Should provide either a storage_account_key or a storage_sas_token")

        params = copy(opts.params)

        params["azure_storage_account_name"] = storage_account_name
        params["azure_container_name"] = container_name

        if !isnothing(storage_account_key)
            params["azure_storage_account_key"] = storage_account_key
        end

        if !isnothing(storage_sas_token)
            params["azure_storage_sas_token"] = storage_sas_token
        end

        if !isnothing(host)
            params["azurite_host"] = host
        end

        return new(Config("az://$(container_name)/", params))
    end
end

as_config(wrapper::AzureConfig) = wrapper.config

"""
    $TYPEDEF

# Keyword Arguments
- `region::String`: AWS S3 region.
- `bucket_name::String`: AWS S3 bucket name.
- `access_key_id::Option{String}`: (Optional) AWS S3 access key id.
- `secret_access_key::Option{String}`: (Optional) AWS S3 secret access key.
- `session_token::Option{String}`: (Optional) AWS S3 session_token.
- `host::Option{String}`: (Optional) Alternative S3 host. For example, if using Minio.
- `opts::ClientOptions`: (Optional) Client configuration options.
"""
struct AwsConfig <: AsConfig
    config::Config
    function AwsConfig(;
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

        return new(Config("s3://$(bucket_name)/", params))
    end
end

as_config(wrapper::AwsConfig) = wrapper.config

function Base.show(io::IO, config::Config)
    dict = copy(JSON3.read(config.config_string))
    print(io, "Config(")
    first = true
    for key in keys(dict)
        if first
            first = false
        else
            print(io, ", ")
        end
        if occursin(r"secret|token|key", string(key))
            print(io, repr(key), " => ", repr("*****"))
        else
            print(io, repr(key), " => ", repr(dict[key]))
        end
    end
    print(io, ")")
end

const _ConfigFFI = NTuple{1,Cstring}

function Base.cconvert(::Type{Ref{Config}}, config::Config)
   config_ffi = (
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, config.config_string)),
    )::_ConfigFFI
    # cconvert ensures its outputs are preserved during a ccall, so we can crate a pointer
    # safely in the unsafe_convert call.
    return config_ffi, Ref(config_ffi)
end
function Base.unsafe_convert(::Type{Ref{Config}}, x::Tuple{T,Ref{T}}) where {T<:_ConfigFFI}
    return Base.unsafe_convert(Ptr{_ConfigFFI}, x[2])
end

include("builder.jl")

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
    get!(buffer, path, conf) -> Int

Send a get request to the object store.

Fetches the data bytes at `path` and writes them to the given `buffer`.

# Arguments
- `buffer::AbstractVector{UInt8}`: The buffer to write the object data to.
  The contents of the buffer will be mutated.
  The buffer must be at least as large as the data.
  The buffer will not be resized.
- `path::String`: The location of the data to fetch.
- `conf::AsConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Returns
- `nbytes::Int`: The number of bytes read from the object store and written to the buffer.
  That is, `buffer[1:nbytes]` will contain the object data.

# Throws
- `GetException`: If the request fails for any reason, including if the `buffer` is too small.
"""
function get!(buffer::AbstractVector{UInt8}, path::String, conf::AsConfig)
    response_ref = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = as_config(conf)
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
    put(buffer, path, conf) -> Int

Send a put request to the object store.

Atomically writes the data bytes in `buffer` to `path`.

# Arguments
- `buffer::AbstractVector{UInt8}`: The data to write to the object store.
  This buffer will not be mutated.
- `path::String`: The location to write data to.
- `conf::AsConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Returns
- `nbytes::Int`: The number of bytes written to the object store.
  Is always equal to `length(buffer)`.

# Throws
- `PutException`: If the request fails for any reason.
"""
function put(buffer::AbstractVector{UInt8}, path::String, conf::AsConfig)
    response_ref = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    config = as_config(conf)
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
