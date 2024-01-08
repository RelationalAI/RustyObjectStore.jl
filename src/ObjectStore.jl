module ObjectStore

export init_object_store, blob_get!, blob_put, AzureCredentials, ObjectStoreConfig

using object_store_ffi_jll
using Base.Libc.Libdl: dlext

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

struct ObjectStoreConfig
    max_retries::Culonglong
    retry_timeout_sec::Culonglong
end

const OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
function init_object_store(config::ObjectStoreConfig = ObjectStoreConfig(15, 150))
    Base.@lock _INIT_LOCK begin
        if OBJECT_STORE_STARTED[]
            return
        end
        res = @ccall rust_lib.start(config::ObjectStoreConfig)::Cint
        if res != 0
            error("Failed to init_object_store")
        end
        OBJECT_STORE_STARTED[] = true
    end
end

struct AzureCredentials
    account::String
    container::String
    key::String
    host::String
end
function Base.show(io::IO, credentials::AzureCredentials)
    print(io, "AzureCredentials("),
    print(io, repr(credentials.account), ", ")
    print(io, repr(credentials.container), ", ")
    print(io, "\"*****\", ") # don't print the secret key
    print(io, repr(credentials.host), ")")
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
            error(err)
        end

        return Int(response.length)
    end
end

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
            error(err)
        end

        return Int(response.length)
    end
end

end # module
