module ObjectStore

const rust_lib_dir = @static if Sys.islinux()
    joinpath(
        @__DIR__,
        "..",
        "..",
        "deps",
        "rust-store",
        "target",
        "release",
    )
elseif Sys.isapple()
    # the release target lives in a different directory because MacOS is built
    # with Rosetta/i386
    joinpath(
        @__DIR__,
        "..",
        "..",
        "deps",
        "rust-store",
        "target",
        "x86_64-apple-darwin",
        "release"
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

RUST_STORE_STARTED = false
function init_rust_store()
    global RUST_STORE_STARTED
    if RUST_STORE_STARTED
        return
    end
    @ccall rust_lib.start()::Cint
    RUST_STORE_STARTED = true
end

struct AzureCredentials
    account::String
    container::String
    key::String
    host::String
end

struct AzureCredentialsFFI
    account::Cstring
    container::Cstring
    key::Cstring
    host::Cstring
end

function to_ffi(credentials::AzureCredentials)
    AzureCredentialsFFI(
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.account)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.container)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.key)),
        Base.unsafe_convert(Cstring, Base.cconvert(Cstring, credentials.host))
    )
end


struct Response
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}

    Response() = new(-1, 0, C_NULL)
end

function blob_get!(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(Response())
    credentials_ffi = Ref(to_ffi(credentials))
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_get(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials_ffi::Ref{AzureCredentialsFFI},
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

function blob_put!(path::String, buffer::AbstractVector{UInt8}, credentials::AzureCredentials)
    response = Ref(Response())
    credentials_ffi = Ref(to_ffi(credentials))
    size = length(buffer)
    cond = Base.AsyncCondition()
    cond_handle = cond.handle
    while true
        res = @ccall rust_lib.perform_put(
            path::Cstring,
            buffer::Ref{Cuchar},
            size::Culonglong,
            credentials_ffi::Ref{AzureCredentialsFFI},
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
