module LibRustStore

using rust_store_jll
export rust_store_jll

using CEnum

@cenum CResult::Int32 begin
    Uninitialized = -1
    Ok = 0
    Error = 1
    Backoff = 2
end

struct GlobalConfigOptions
    max_retries::Csize_t
    retry_timeout_sec::UInt64
end

struct AzureCredentials
    account::Ptr{Cchar}
    container::Ptr{Cchar}
    key::Ptr{Cchar}
    host::Ptr{Cchar}
end

struct Response
    result::CResult
    length::Csize_t
    error_message::Ptr{Int8}
end

function uv_async_send(cond)
    ccall((:uv_async_send, librust_store), Int32, (Ptr{Cvoid},), cond)
end

function start(config)
    ccall((:start, librust_store), CResult, (GlobalConfigOptions,), config)
end

function perform_get(path, buffer, size, credentials, response, handle)
    ccall((:perform_get, librust_store), CResult, (Ptr{Cchar}, Ptr{UInt8}, Csize_t, Ptr{AzureCredentials}, Ptr{Response}, Ptr{Cvoid}), path, buffer, size, credentials, response, handle)
end

function perform_put(path, buffer, size, credentials, response, handle)
    ccall((:perform_put, librust_store), CResult, (Ptr{Cchar}, Ptr{UInt8}, Csize_t, Ptr{AzureCredentials}, Ptr{Response}, Ptr{Cvoid}), path, buffer, size, credentials, response, handle)
end

function destroy_cstring(string)
    ccall((:destroy_cstring, librust_store), CResult, (Ptr{Cchar},), string)
end

end # module
