module ObjectStore

using object_store_ffi_jll: libobject_store_ffi

include("generated/LibObjectStore.jl")
using .LibObjectStore

export init_object_store, blob_get!, blob_put, AzureCredentials, ObjectStoreConfig

"""
    ObjectStoreConfig(max_retries, retry_timeout_sec)

Global configuration options to be passed to `init_object_store`.
"""
const ObjectStoreConfig = FFI_GlobalConfigOptions


const OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
function init_object_store(config::ObjectStoreConfig=ObjectStoreConfig(15, 150))
    Base.@lock _INIT_LOCK begin
        if OBJECT_STORE_STARTED[]
            return
        end
        res = @ccall libobject_store_ffi.start(config::FFI_GlobalConfigOptions)::Cint
        if res != 0
            error("Failed to init_rust_store")
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


function Base.cconvert(::Type{Ref{AzureCredentials}}, credentials::AzureCredentials)
   credentials_ffi = FFI_AzureCredentials(
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.account)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.container)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.key)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.host))
    )
    # cconvert ensures its outputs are preserved during a ccall, so we can crate a pointer
    # safely in the unsafe_convert call.
    return credentials_ffi, Ref(credentials_ffi)
end
function Base.unsafe_convert(::Type{Ref{AzureCredentials}}, x::Tuple{T,Ref{T}}) where {T<:FFI_AzureCredentials}
    return Base.unsafe_convert(Ptr{FFI_AzureCredentials}, x[2])
end


function blob_get!(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(FFI_Response(LibObjectStore.Uninitialized, 0, C_NULL))
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall libobject_store_ffi.perform_get(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials::Ref{AzureCredentials},
            response::Ref{FFI_Response},
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
            @ccall libobject_store_ffi.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            error(err)
        end

        return Int(response.length)
    end
end

function blob_put(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(FFI_Response(LibObjectStore.Uninitialized, 0, C_NULL))
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall libobject_store_ffi.perform_put(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials::Ref{AzureCredentials},
            response::Ref{FFI_Response},
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
            @ccall libobject_store_ffi.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
            error(err)
        end

        return Int(response.length)
    end
end

end # module
