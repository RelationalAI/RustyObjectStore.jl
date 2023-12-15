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

# This function will be called if Rust panics. It should only happen in the
# case of a program bug (not, e.g., a network failure)
function panic_handler()
    # Notify the error listener to throw an exception on the Julia side
    ccall(:uv_async_send, Nothing, (Ptr{Nothing},), panic_cond[].handle)
    return
end
const panic_handler_c = Ref{Ptr{Cvoid}}(C_NULL)
const panic_cond = Ref{Base.AsyncCondition}()

function __init__()
    # Listen for Rust panics from panic_handler
    panic_cond[] = Base.AsyncCondition() do _
        @error "Rust panic"
    end
    panic_handler_c[] = @cfunction(panic_handler, Cvoid, ())
end

const RUST_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
function init_rust_store(config::RustStoreConfig = RustStoreConfig(15, 150))
    Base.@lock _INIT_LOCK begin
        if RUST_STORE_STARTED[]
            return
        end
        Base.@ccall rust_lib.start(panic_handler_c[]::Ptr{Cvoid}, config::RustStoreConfig)::Cint
        RUST_STORE_STARTED[] = true
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
