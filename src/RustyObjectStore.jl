module RustyObjectStore

export init_object_store, blob_get!, blob_put, AzureCredentials, ObjectStoreConfig

using Base.Libc.Libdl: dlext
using Base: @kwdef, @lock
using DocStringExtensions
using object_store_ffi_jll

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
@kwdef struct ObjectStoreConfig
    "The maximum number of times to retry a request."
    max_retries::Culonglong
    "The number of seconds from the initial request after which no further retries will be attempted."
    retry_timeout_sec::Culonglong
end

function Base.show(io::IO, config::ObjectStoreConfig)
    print(io, "ObjectStoreConfig("),
    print(io, "max_retries=", Int(config.max_retries), ", ")
    print(io, "retry_timeout_sec=", Int(config.retry_timeout_sec), ")")
end

const DEFAULT_CONFIG = ObjectStoreConfig(max_retries=15, retry_timeout_sec=150)

const _OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()

struct InitException <: Exception
    msg::String
    return_code::Cint
end

"""
    init_object_store()
    init_object_store(config::ObjectStoreConfig)

Initialise object store.

This starts a `tokio` runtime for handling `object_store` requests.
It must be called before sending a request e.g. with `blob_get!` or `blob_put`.
The runtime is only started once and cannot be re-initialised with a different config,
subsequent `init_object_store` calls have no effect.

# Throws
- `InitException`: if the runtime fails to start.
"""
function init_object_store(config::ObjectStoreConfig=DEFAULT_CONFIG)
    @lock _INIT_LOCK begin
        if _OBJECT_STORE_STARTED[]
            return nothing
        end
        res = @ccall rust_lib.start(config::ObjectStoreConfig)::Cint
        if res != 0
            throw(InitException("Failed to initialise object store runtime.", res))
        end
        _OBJECT_STORE_STARTED[] = true
    end
    return nothing
end

"""
    $TYPEDEF

# Arguments
$TYPEDFIELDS
"""
@kwdef struct AzureCredentials
    "Azure account name"
    account::String
    "Azure container name"
    container::String
    "Azure access key"
    key::String
    "(Optional) Alternative Azure host. For example, if using Azurite."
    host::String=""
end
function Base.show(io::IO, credentials::AzureCredentials)
    print(io, "AzureCredentials("),
    print(io, repr(credentials.account), )
    print(io, ", ", repr(credentials.container))
    print(io, ", ", "\"*****\"") # don't print the secret key
    !isempty(credentials.host) && print(io, ", ", repr(credentials.host))
    print(io, ")")
end

const _AzureCredentialsFFI = NTuple{4,Cstring}

function Base.cconvert(::Type{Ref{AzureCredentials}}, credentials::AzureCredentials)
   credentials_ffi = (
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.account)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.container)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.key)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.host))
    )::_AzureCredentialsFFI
    # cconvert ensures its outputs are preserved during a ccall, so we can crate a pointer
    # safely in the unsafe_convert call.
    return credentials_ffi, Ref(credentials_ffi)
end

function Base.unsafe_convert(::Type{Ref{AzureCredentials}}, x::Tuple{T,Ref{T}}) where {T<:_AzureCredentialsFFI}
    return Base.unsafe_convert(Ptr{_AzureCredentialsFFI}, x[2])
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

# TODO: this should be `blob_get!(buffer, path, credentials)` i.e. mutated argument first.
"""
    blob_get!(path, buffer, credentials) -> Int

Send a get request to Azure Blob Storage.

Fetches the data bytes at `path` and writes them to the given `buffer`.

# Arguments
- `path::String`: The location of the data to fetch.
- `buffer::AbstractVector{UInt8}`: The buffer to write the blob data to.
  The contents of the buffer will be mutated.
  The buffer must be at least as large as the data.
  The buffer will not be resized.
- `credentials::AzureCredentials`: The credentials to use for the request.

# Returns
- `nbytes::Int`: The number of bytes read from Blob Storage and written to the buffer.
  That is, `buffer[1:nbytes]` will contain the blob data.

# Throws
- `GetException`: If the request fails for any reason, including if the `buffer` is too small.
"""
function blob_get!(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_get(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials::Ref{AzureCredentials},
            response::Ref{Response},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if res == 1
            @error "failed to submit get, internal channel closed"
            return 1
        elseif res == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response[]
        if response.result == 1
            err = "failed to process get with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            throw(GetException(err))
        end

        return Int(response.length)
    end
end

# TODO: this should be `blob_put(buffer, path, credentials)` so match `blob_get!`
# when that is changed to put its mutated argument first.
"""
    blob_put(path, buffer, credentials) -> Int

Send a put request to Azure Blob Storage.

Atomically writes the data bytes in `buffer` to `path`.

# Arguments
- `path::String`: The location to write data to.
- `buffer::AbstractVector{UInt8}`: The data to write to Blob Storage.
  This buffer will not be mutated.
- `credentials::AzureCredentials`: The credentials to use for the request.

# Returns
- `nbytes::Int`: The number of bytes written to Blob Storage.
  Is always equal to `length(buffer)`.

# Throws
- `PutException`: If the request fails for any reason.
"""
function blob_put(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_put(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials::Ref{AzureCredentials},
            response::Ref{Response},
            cond_handle::Ptr{Cvoid}
        )::Cint

        if res == 1
            @error "failed to submit put, internal channel closed"
            return 1
        elseif res == 2
            # backoff
            sleep(1.0)
            continue
        end

        wait(cond)

        response = response[]
        if response.result == 1
            err = "failed to process put with error: $(unsafe_string(response.error_message))"
            @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            throw(PutException(err))
        end

        return Int(response.length)
    end
end

end # module
