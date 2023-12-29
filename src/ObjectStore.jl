module ObjectStore


export init_rust_store, blob_get!, blob_put, AzureCredentials, RustStoreConfig

const rust_lib_dir = @static if Sys.islinux() || Sys.isapple()
    joinpath(
        @__DIR__,
        "..",
        "deps",
        "rust_store",
        "target",
        "release",
    )
elseif Sys.iswindows()
    @warn("The rust-store library is currently unsupported on Windows.")
end

const extension = @static if Sys.islinux()
    "so"
elseif Sys.isapple()
    "dylib"
elseif Sys.iswindows()
    "dll"
end

const rust_lib = joinpath(rust_lib_dir, "librust_store.$extension")

struct RustStoreConfig
    max_retries::Culonglong
    retry_timeout_sec::Culonglong
end

const RUST_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
function init_rust_store(config::RustStoreConfig = RustStoreConfig(15, 150))
    Base.@lock _INIT_LOCK begin
        if RUST_STORE_STARTED[]
            return
        end
        res = @ccall rust_lib.start(config::RustStoreConfig)::Cint
        if res != 0
            error("Failed to init_rust_store")
        end
        RUST_STORE_STARTED[] = true
    end
end

struct AzureConnection
    account::String
    container::String
    access_key::String
    host::String
    max_retries::UInt64
    retry_timeout_sec::UInt64

    # Constructor with default max_retries and timeout
    AzureConnection(account::String, container::String, access_key::String, host::String;
                    max_retries=0, retry_timeout_sec=0) =
        new(account, container, access_key, host, 0, 0)

    # Constructor for anonymous (no key) access
    AzureConnection(account::String, container::String, host::String;
                    max_retries=0, retry_timeout_sec=0) =
        new(account, container, "", host, max_retries, retry_timeout_sec)
end



function Base.show(io::IO, connection::AzureConnection)
    print(io, "AzureCredentials("),
    print(io, repr(credentials.account), ", ")
    print(io, repr(credentials.container), ", ")
    print(io, "\"*****\", ") # don't print the secret key
    print(io, repr(credentials.host), ", ")
    print(io, repr(credentials.max_retries), ", ")
    print(io, repr(credentials.retry_timeout_sec), ")")
end

const _AzureConnectionFFI = Tuple{Cstring, Cstring, Cstring, Cstring, Culonglong, Culonglong}

function Base.cconvert(::Type{Ref{AzureConnection}}, connection::AzureConnection)
   connection_ffi = (
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, connection.account)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, connection.container)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, connection.access_key)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, connection.host)),
        connection.max_retries,
        connection.retry_timeout_sec
    )::_AzureConnectionFFI
    # cconvert ensures its outputs are preserved during a ccall, so we can crate a pointer
    # safely in the unsafe_convert call.
    return connection_ffi, Ref(connection_ffi)
end
function Base.unsafe_convert(::Type{Ref{AzureConnection}}, x::Tuple{T,Ref{T}}) where {T<:_AzureConnectionFFI}
    return Base.unsafe_convert(Ptr{_AzureConnectionFFI}, x[2])
end

struct Response
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}

    Response() = new(-1, 0, C_NULL)
end

function blob_get!(path::String, buffer::AbstractVector{UInt8}, connection::AzureConnection)
    response = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_get(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            connection::Ref{AzureConnection},
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

function blob_put(path::String, buffer::AbstractVector{UInt8}, connection::AzureConnection)
    response = Ref(Response())
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_put(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            connection::Ref{AzureConnection},
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
