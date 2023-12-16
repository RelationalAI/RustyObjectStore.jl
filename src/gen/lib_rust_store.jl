module LibRustStore

using rust_store_jll
export rust_store_jll

using CEnum

# The following code is auto-generated. See the `gen/` directory.
# Editting it by hand is not recommended.
# Instead, edit the generating script and then re-generate this file.


@cenum CResult::Int32 begin
    Uninitialized = -1
    Ok = 0
    Error = 1
    Backoff = 2
end

struct FFI_GlobalConfigOptions
    max_retries::Csize_t
    retry_timeout_sec::UInt64
end

struct FFI_AzureCredentials
    account::Ptr{Cchar}
    container::Ptr{Cchar}
    key::Ptr{Cchar}
    host::Ptr{Cchar}
end

struct FFI_Response
    result::CResult
    length::Csize_t
    error_message::Ptr{Int8}
end

# exports
const PREFIXES = ["FFI_"]
for name in names(@__MODULE__; all=true), prefix in PREFIXES
    if startswith(string(name), prefix)
        @eval export $name
    end
end

end # module
