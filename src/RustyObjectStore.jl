module RustyObjectStore

export init_object_store, get_object!, put_object, delete_object
export StaticConfig, ClientOptions, Config, AzureConfig, AWSConfig, SnowflakeConfig
export status_code, is_connection, is_timeout, is_early_eof, is_unknown, is_parse_url
export get_object_stream, ReadStream, finish!
export put_object_stream, WriteStream, cancel!, shutdown!
export current_metrics
export max_entries_per_chunk, ListEntry, list_objects, list_objects_stream, next_chunk!

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
    "Put requests with a size in bytes greater than this will use multipart operations"
    multipart_put_threshold::Culonglong
    "Get requests with a size in bytes greater than this will use multipart operations"
    multipart_get_threshold::Culonglong
    "The size in bytes for each part of multipart get operations"
    multipart_get_part_size::Culonglong
    "The max number of allowed Rust request tasks"
    concurrency_limit::Cuint
end

function Base.show(io::IO, config::StaticConfig)
    print(io, "StaticConfig("),
    print(io, "n_threads=", Int(config.n_threads), ",")
    print(io, "cache_capacity=", Int(config.cache_capacity), ",")
    print(io, "cache_ttl_secs=", Int(config.cache_ttl_secs), ",")
    print(io, "cache_tti_secs=", Int(config.cache_tti_secs), ",")
    print(io, "multipart_put_threshold=", Int(config.multipart_put_threshold), ",")
    print(io, "multipart_get_threshold=", Int(config.multipart_get_threshold), ",")
    print(io, "multipart_get_part_size=", Int(config.multipart_get_part_size), ")")
end

const DEFAULT_CONFIG = StaticConfig(
    n_threads=0,
    cache_capacity=100,
    cache_ttl_secs=30 * 60,
    cache_tti_secs=5 * 60,
    multipart_put_threshold=10 * 1024 * 1024,
    multipart_get_threshold=8 * 1024 * 1024,
    multipart_get_part_size=8 * 1024 * 1024,
    concurrency_limit=512
)

function default_panic_hook()
    println("Rust thread panicked, exiting the process")
    exit(1)
end

const _OBJECT_STORE_STARTED = Ref(false)
const _INIT_LOCK::ReentrantLock = ReentrantLock()
_PANIC_HOOK::Function = default_panic_hook

struct InitException <: Exception
    msg::String
    return_code::Cint
end

Base.@ccallable function panic_hook_wrapper()::Cint
    global _PANIC_HOOK
    _PANIC_HOOK()
    return 0
end

# This is the callback that Rust calls to notify a Julia task of a completed operation.
# The argument is transparent to Rust and is simply what gets passed from Julia in the handle
# argument of the @ccall. Currently we pass a pointer to a Base.Event that must be notified to
# wakeup the appropriate task.
Base.@ccallable function notify_result(event_ptr::Ptr{Nothing})::Cint
    event = unsafe_pointer_to_objref(event_ptr)::Base.Event
    notify(event)
    return 0
end

# A dict of all tasks that are waiting some result from Rust
# and should thus not be garbage collected.
# This copies the behavior of Base.preserve_handle.
const tasks_in_flight = IdDict{Task, Int64}()
const preserve_task_lock = Threads.SpinLock()
function preserve_task(x::Task)
    @lock preserve_task_lock begin
        v = get(tasks_in_flight, x, 0)::Int
        tasks_in_flight[x] = v + 1
    end
    nothing
end
function unpreserve_task(x::Task)
    @lock preserve_task_lock begin
        v = get(tasks_in_flight, x, 0)::Int
        if v == 0
            error("unbalanced call to unpreserve_task for $(typeof(x))")
        elseif v == 1
            pop!(tasks_in_flight, x)
        else
            tasks_in_flight[x] = v - 1
        end
    end
    nothing
end

"""
    init_object_store()
    init_object_store(config::StaticConfig)
    init_object_store(config::StaticConfig; on_rust_panic::Function)

Initialise object store.

This starts a `tokio` runtime for handling `object_store` requests.
It must be called before sending a request e.g. with `get_object!` or `put_object`.
The runtime is only started once and cannot be re-initialised with a different config,
subsequent `init_object_store` calls have no effect.

An optional panic hook may be provided to react to panics on Rust's native threads.
The default behavior is to log and exit the process.

# Throws
- `InitException`: if the runtime fails to start.
"""
function init_object_store(
    config::StaticConfig=DEFAULT_CONFIG;
    on_rust_panic::Function=default_panic_hook
)
    global _PANIC_HOOK
    @lock _INIT_LOCK begin
        if _OBJECT_STORE_STARTED[]
            return nothing
        end
        _PANIC_HOOK = on_rust_panic
        panic_fn_ptr = @cfunction(panic_hook_wrapper, Cint, ())
        fn_ptr = @cfunction(notify_result, Cint, (Ptr{Nothing},))
        res = @ccall rust_lib.start(config::StaticConfig, panic_fn_ptr::Ptr{Nothing}, fn_ptr::Ptr{Nothing})::Cint
        if res != 0
            throw(InitException("Failed to initialise object store runtime.", res))
        end
        _OBJECT_STORE_STARTED[] = true
    end
    return nothing
end

macro option_print(obj, name, hide = false)
    return esc(:( !isnothing($obj.$name)
        && print(io, ", ", $(string(name)), "=", $hide ? "*****" : repr($obj.$name)) ))
end

function response_error_to_string(response, operation)
    err = string("failed to process ", operation, " with error: ", unsafe_string(response.error_message))
    @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
    return err
end

macro throw_on_error(response, operation, exception)
    throw_on_error(response, operation, exception)
end

function throw_on_error(response, operation, exception)
    return :( $(esc(:($response.result == 1))) ? throw($exception($response_error_to_string($(esc(response)), $operation))) : $(nothing) )
end

function ensure_wait(event::Base.Event)
    for i in 1:20
        try
            return wait(event)
        catch e
            @error "cannot skip this wait point to prevent UB, ignoring exception: $(e)"
        end
    end

    @error "ignored too many wait exceptions, giving up"
    exit(1)
end

function wait_or_cancel(event::Base.Event, response)
    try
        return wait(event)
    catch e
        @ccall rust_lib.cancel_context(response.context::Ptr{Cvoid})::Cint
        ensure_wait(event)
        @ccall rust_lib.destroy_cstring(response.error_message::Ptr{Cchar})::Cint
        rethrow()
    finally
        @ccall rust_lib.destroy_context(response.context::Ptr{Cvoid})::Cint
    end
end

"""
    $TYPEDEF

# Keyword Arguments
- `request_timeout_secs::Option{Int}`: (Optional) Client request timeout in seconds.
- `connect_timeout_secs::Option{Int}`: (Optional) Client connection timeout in seconds.
- `max_retries::Option{Int}`: (Optional) Maximum number of retry attempts.
- `retry_timeout_secs::Option{Int}`: (Optional) Maximum amount of time from the initial request after which no further retries will be attempted (in seconds).
- `initial_backoff_ms::Option{Int}`: (Optional) Initial delay for exponential backoff (in milliseconds).
- `max_backoff_ms::Option{Int}`: (Optional) Maximum delay for exponential backoff (in milliseconds).
- `backoff_exp_base::Option{Float64}`: (Optional) The base of the exponential for backoff delay calculations.
"""
struct ClientOptions
    request_timeout_secs::Option{Int}
    connect_timeout_secs::Option{Int}
    max_retries::Option{Int}
    retry_timeout_secs::Option{Int}
    initial_backoff_ms::Option{Int}
    max_backoff_ms::Option{Int}
    backoff_exp_base::Option{Float64}
    params::Dict{String, String}

    function ClientOptions(;
        request_timeout_secs::Option{Int} = nothing,
        connect_timeout_secs::Option{Int} = nothing,
        max_retries::Option{Int} = nothing,
        retry_timeout_secs::Option{Int} = nothing,
        initial_backoff_ms::Option{Int} = nothing,
        max_backoff_ms::Option{Int} = nothing,
        backoff_exp_base::Option{Float64} = nothing,
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

        if !isnothing(initial_backoff_ms)
            # `ms` suffix is not required as this field is already expected to be the number of milliseconds
            params["initial_backoff_ms"] = string(initial_backoff_ms)
        end

        if !isnothing(max_backoff_ms)
            # `ms` suffix is not required as this field is already expected to be the number of milliseconds
            params["max_backoff_ms"] = string(max_backoff_ms)
        end

        if !isnothing(backoff_exp_base)
            params["backoff_exp_base"] = string(backoff_exp_base)
        end

        return new(
            request_timeout_secs,
            connect_timeout_secs,
            max_retries,
            retry_timeout_secs,
            initial_backoff_ms,
            max_backoff_ms,
            backoff_exp_base,
            params
        )
    end
end

function Base.show(io::IO, opts::ClientOptions)
    print(io, "ClientOptions("),
    @option_print(opts, request_timeout_secs)
    @option_print(opts, connect_timeout_secs)
    @option_print(opts, max_retries)
    @option_print(opts, retry_timeout_secs)
    @option_print(opts, initial_backoff_ms)
    @option_print(opts, max_backoff_ms)
    @option_print(opts, backoff_exp_base)
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

        if isnothing(storage_account_key) && isnothing(storage_sas_token)
            params["azure_skip_signature"] = "true"
        end

        map!(v -> strip(v), values(params))
        cached_config = Config("az://$(strip(container_name))/", params)
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
    use_instance_metadata::Bool
    host::Option{String}
    opts::ClientOptions
    cached_config::Config
    function AWSConfig(;
        region::String,
        bucket_name::String,
        access_key_id::Option{String} = nothing,
        secret_access_key::Option{String} = nothing,
        session_token::Option{String} = nothing,
        use_instance_metadata::Bool = false,
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
        else
            params["aws_virtual_hosted_style_request"] = "true"
        end

        if !use_instance_metadata && isnothing(access_key_id)
            params["aws_skip_signature"] = "true"
        end

        if use_instance_metadata && (!isnothing(access_key_id) || !isnothing(secret_access_key))
            error("Credentials should not be provided when using instance metadata")
        end

        map!(v -> strip(v), values(params))
        cached_config = Config("s3://$(strip(bucket_name))/", params)
        return new(
            region,
            bucket_name,
            access_key_id,
            secret_access_key,
            session_token,
            use_instance_metadata,
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
    conf.use_instance_metadata && print(io, "use_instance_metadata=", repr(conf.use_instance_metadata))
    @option_print(conf, host)
    print(io, ", ", "opts=", repr(conf.opts), ")")
end

"""
    $TYPEDEF

Configuration for the Snowflake stage object store backend.

It is recommended to reuse an instance for many operations.

# Keyword Arguments
- `stage::String`: Snowflake stage
- `encryption_scheme::Option{String}`: (Optional) Encryption scheme to enforce (one of AES_128_CBC, AES_256_GCM)
- `account::Option{String}`: (Optional) Snowflake account (read from SNOWFLAKE_ACCOUNT env var if missing)
- `database::Option{String}`: (Optional) Snwoflake database (read from SNOWFLAKE_DATABASE env var if missing)
- `schema::Option{String}`: (Optional) Snowflake schema (read from SNOWFLAKE_SCHEMA env var if missing)
- `endpoint::Option{String}`: (Optional) Snowflake endpoint (read from SNOWFLAKE_ENDPOINT or SNOWFLAKE_HOST env vars if missing)
- `warehouse::Option{String}`: (Optional) Snowflake warehouse
- `username::Option{String}`: (Optional) Snowflake username (required for user/pass flow)
- `password::Option{String}`: (Optional) Snowflake password (required for user/pass flow)
- `role::Option{String}`: (Optional) Snowflake role (required for user/pass flow)
- `master_token_path::Option{String}`: (Optional) Path to Snowflake master token (read from MASTER_TOKEN_PATH or defaults to `/snowflake/session/token` if missing)
- `keyring_capacity::Option{Int}`: (Optional) Maximum number of keys to be kept in the in-memory keyring (key cache)
- `keyring_ttl_secs::Option{Int}`: (Optional) Duration in seconds after which a key is removed from the keyring
- `opts::ClientOptions`: (Optional) Client configuration options.
"""
struct SnowflakeConfig <: AbstractConfig
    stage::String
    encryption_scheme::Option{String}
    account::Option{String}
    database::Option{String}
    schema::Option{String}
    endpoint::Option{String}
    warehouse::Option{String}
    username::Option{String}
    password::Option{String}
    role::Option{String}
    master_token_path::Option{String}
    keyring_capacity::Option{Int}
    keyring_ttl_secs::Option{Int}
    opts::ClientOptions
    cached_config::Config
    function SnowflakeConfig(;
        stage::String,
        encryption_scheme::Option{String} = nothing,
        account::Option{String} = nothing,
        database::Option{String} = nothing,
        schema::Option{String} = nothing,
        endpoint::Option{String} = nothing,
        warehouse::Option{String} = nothing,
        username::Option{String} = nothing,
        password::Option{String} = nothing,
        role::Option{String} = nothing,
        master_token_path::Option{String} = nothing,
        keyring_capacity::Option{Int} = nothing,
        keyring_ttl_secs::Option{Int} = nothing,
        opts::ClientOptions = ClientOptions()
    )
        params = copy(opts.params)

        params["snowflake_stage"] = stage

        if !isnothing(encryption_scheme)
            params["snowflake_encryption_scheme"] = encryption_scheme
        end

        if !isnothing(account)
            params["snowflake_account"] = account
        end

        if !isnothing(database)
            params["snowflake_database"] = database
        end

        if !isnothing(schema)
            params["snowflake_schema"] = schema
        end

        if !isnothing(endpoint)
            params["snowflake_endpoint"] = endpoint
        end

        if !isnothing(warehouse)
            params["snowflake_warehouse"] = warehouse
        end

        if !isnothing(username)
            params["snowflake_username"] = username
        end

        if !isnothing(password)
            params["snowflake_password"] = password
        end

        if !isnothing(role)
            params["snowflake_role"] = role
        end

        if !isnothing(master_token_path)
            params["snowflake_master_token_path"] = master_token_path
        end

        if !isnothing(keyring_capacity)
            params["snowflake_keyring_capacity"] = string(keyring_capacity)
        end

        if !isnothing(keyring_ttl_secs)
            params["snowflake_keyring_ttl_secs"] = string(keyring_ttl_secs)
        end

        map!(v -> strip(v), values(params))
        cached_config = Config("snowflake://$(strip(stage))/", params)
        return new(
            stage,
            encryption_scheme,
            account,
            database,
            schema,
            endpoint,
            warehouse,
            username,
            password,
            role,
            master_token_path,
            keyring_capacity,
            keyring_ttl_secs,
            opts,
            cached_config
        )
    end
end

into_config(conf::SnowflakeConfig) = conf.cached_config

function Base.show(io::IO, conf::SnowflakeConfig)
    print(io, "SnowflakeConfig("),
    print(io, "stage=", repr(conf.stage))
    @option_print(conf, encryption_scheme)
    @option_print(conf, account)
    @option_print(conf, database)
    @option_print(conf, schema)
    @option_print(conf, endpoint)
    @option_print(conf, warehouse)
    @option_print(conf, username)
    @option_print(conf, password, true)
    @option_print(conf, role)
    @option_print(conf, keyring_capacity)
    @option_print(conf, keyring_ttl_secs)
    print(io, ", ", "opts=", repr(conf.opts), ")")
end

mutable struct Response
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    Response() = new(-1, 0, C_NULL, C_NULL)
end

abstract type ErrorReason end

struct ConnectionError <: ErrorReason end
struct StatusError <: ErrorReason
    code::Int
end
struct EarlyEOF <: ErrorReason end
struct TimeoutError <: ErrorReason end
struct ParseURLError <: ErrorReason end
struct UnknownError <: ErrorReason end

reason_description(::ConnectionError) = "Connection"
reason_description(r::StatusError) = "StatusCode($(r.code))"
reason_description(::EarlyEOF) = "EarlyEOF"
reason_description(::TimeoutError) = "Timeout"
reason_description(::ParseURLError) = "ParseURL"
reason_description(::UnknownError) = "Unknown"

abstract type RequestException <: Exception end
struct GetException <: RequestException
    msg::String
    reason::ErrorReason

    GetException(msg) = new(msg, rust_message_to_reason(msg))
end
struct PutException <: RequestException
    msg::String
    reason::ErrorReason

    PutException(msg) = new(msg, rust_message_to_reason(msg))
end
struct DeleteException <: RequestException
    msg::String
    reason::ErrorReason

    DeleteException(msg) = new(msg, rust_message_to_reason(msg))
end
struct ListException <: RequestException
    msg::String
    reason::ErrorReason

    ListException(msg) = new(msg, rust_message_to_reason(msg))
end


message(e::GetException) = e.msg::String
message(e::PutException) = e.msg::String
message(e::ListException) = e.msg::String
message(e::DeleteException) = e.msg::String

function message(e::Exception)
    iobuf = IOBuffer()
    Base.showerror(iobuf, e)
    return String(take!(iobuf))
end

reason(e::GetException) = e.reason::ErrorReason
reason(e::PutException) = e.reason::ErrorReason
reason(e::ListException) = e.reason::ErrorReason
reason(e::DeleteException) = e.reason::ErrorReason
reason(e::Exception) = UnknownError()

function status_code(e::Exception)
    return reason(e) isa StatusError ? reason(e).code : nothing
end

function is_connection(e::Exception)
    return reason(e) isa ConnectionError
end

function is_timeout(e::Exception)
    return reason(e) isa TimeoutError
end

function is_early_eof(e::Exception)
    return reason(e) isa EarlyEOF
end

function is_parse_url(e::Exception)
    return reason(e) isa ParseURLError
end

function is_unknown(e::Exception)
    return reason(e) isa UnknownError
end

function safe_message(e::Exception)
    if e isa RequestException
        msg = message(e)
        r = reason(e)
        if contains(msg, "<Error>") || contains(msg, "http")
            # Contains unreadacted payload from backend or urls, try extracting safe information
            code, backend_msg, report = extract_safe_parts(message(e))
            reason_str = reason_description(r)

            code = isnothing(code) ? "Unknown" : code
            backend_msg = isnothing(backend_msg) ? "Error without safe message" : backend_msg
            retry_report = isnothing(report) ? "" : "\n\n$(report)"

            return "$(backend_msg) (code: $(code), reason: $(reason_str))$(retry_report)"
        else
            # Assuming it safe as it does not come from backend or has url, return message directly
            return msg
        end
    else
        return nothing
    end
end

function rust_message_to_reason(msg::AbstractString)
    if (
        contains(msg, "connection error")
        || contains(msg, "tcp connect error")
        || contains(msg, "error trying to connect")
        || contains(msg, "client error (Connect)")
       ) && (
        contains(msg, "deadline has elapsed")
        || contains(msg, "Connection refused")
        || contains(msg, "Connection reset by peer")
        || contains(msg, "dns error")
       )
        return ConnectionError()
    elseif contains(msg, "Client error with status")
        m = match(r"Client error with status (\d+) ", msg)
        if !isnothing(m)
            code = tryparse(Int, m.captures[1])
            if !isnothing(code)
                return StatusError(code)
            else
                return UnknownError()
            end
        else
            return UnknownError()
        end
    elseif contains(msg, "HTTP status server error")
        m = match(r"HTTP status server error \((\d+) ", msg)
        if !isnothing(m)
            code = tryparse(Int, m.captures[1])
            if !isnothing(code)
                return StatusError(code)
            else
                return UnknownError()
            end
        else
            return UnknownError()
        end
    elseif contains(msg, "connection closed before message completed") ||
        contains(msg, "end of file before message length reached") ||
        contains(msg, "Connection reset by peer")
        return EarlyEOF()
    elseif contains(msg, "timed out")
        return TimeoutError()
    elseif contains(msg, "Unable to convert URL") ||
        contains(msg, "Unable to recognise URL")
        return ParseURLError()
    else
        return UnknownError()
    end
end

function extract_safe_parts(msg::AbstractString)
    code = nothing
    backend_message = nothing
    retry_report = nothing
    codemsg = match(r"<Error>[\s\S]*?<Code>([\s\S]*?)</Code>[\s\S]*?<Message>([\s\S]*?)(?:</Message>|\n)", msg)
    if !isnothing(codemsg)
        code = codemsg.captures[1]
        backend_message = codemsg.captures[2]
    end
    retry_match = match(r"Recent attempts \([\s\S]*", msg)
    if !isnothing(retry_match)
        retry_report = retry_match.match
    end

    return (code, backend_message, retry_report)
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
    response = Response()
    size = length(buffer)
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    while true
        preserve_task(ct)
        result = GC.@preserve buffer config response event try
            result = @ccall rust_lib.get(
                path::Cstring,
                buffer::Ref{Cuchar},
                size::Culonglong,
                config::Ref{Config},
                response::Ref{Response},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "get", GetException)

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
    response = Response()
    size = length(buffer)
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    while true
        preserve_task(ct)
        result = GC.@preserve buffer config response event try
            result = @ccall rust_lib.put(
                path::Cstring,
                buffer::Ref{Cuchar},
                size::Culonglong,
                config::Ref{Config},
                response::Ref{Response},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "put", PutException)

        return Int(response.length)
    end
end

"""
    delete_object(path, conf)

Send a delete request to the object store.

# Arguments
- `path::String`: The location of the object to delete.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Throws
- `DeleteException`: If the request fails for any reason. Note that S3 will treat a delete request
  to a non-existing object as a success, while Azure Blob will treat it as a 404 error.
"""
function delete_object(path::String, conf::AbstractConfig)
    response = Response()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.delete(
                path::Cstring,
                config::Ref{Config},
                response::Ref{Response},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        @throw_on_error(response, "delete_object", DeleteException)

        return nothing
    end
end

mutable struct ReadResponseFFI
    result::Cint
    length::Culonglong
    eof::Cuchar
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ReadResponseFFI() = new(-1, 0, 0, C_NULL, C_NULL)
end

mutable struct ReadStreamResponseFFI
    result::Cint
    stream::Ptr{Nothing}
    object_size::Culonglong
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ReadStreamResponseFFI() = new(-1, C_NULL, 0, C_NULL, C_NULL)
end

"""
    ReadStream


Opaque IO stream of object data.

It is necessary to `Base.close` the stream if it is not run to completion.

"""
mutable struct ReadStream <: IO
    ptr::Ptr{Nothing}
    object_size::Int
    bytes_read::Int
    ended::Bool
    error::Option{String}
end

function Base.eof(io::ReadStream)
    if io.ended
        return true
    elseif !isnothing(io.error)
        throw("stream stopped by prevoius error: $(io.error)")
    elseif bytesavailable(io) > 0
        return false
    else
        response = ReadResponseFFI()
        ct = current_task()
        event = Base.Event()
        handle = pointer_from_objref(event)
        preserve_task(ct)
        GC.@preserve io response event try
            result = @ccall rust_lib.is_end_of_stream(
                io.ptr::Ptr{Cvoid},
                response::Ref{ReadResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            @assert result == 0

            wait_or_cancel(event, response)
        finally
            unpreserve_task(ct)
        end

        try
            @throw_on_error(response, "is_end_of_stream", GetException)
        catch e
            stream_error!(io, e.msg)
            rethrow()
        end

        eof = response.eof > 0

        if eof
            stream_end!(io)
        end

        return eof
    end
end
function Base.bytesavailable(io::ReadStream)
    if !Base.isopen(io)
        return 0
    else
        result = @ccall rust_lib.bytes_available(io.ptr::Ptr{Cvoid})::Clonglong
        @assert result >= 0
        return Int(result)
    end
end
function Base.close(io::ReadStream)
    finish!(io)
    return nothing
end

Base.isopen(io::ReadStream) = !io.ended && isnothing(io.error)
Base.iswritable(io::ReadStream) = false
Base.filesize(io::ReadStream) = io.object_size

function stream_end!(io::ReadStream)
    @assert Base.isopen(io)
    io.ended = true
    @ccall rust_lib.destroy_read_stream(io.ptr::Ptr{Nothing})::Cint
end

function stream_error!(io::ReadStream, err::String)
    @assert Base.isopen(io)
    io.error = err
    @ccall rust_lib.destroy_read_stream(io.ptr::Ptr{Nothing})::Cint
end

function Base.readbytes!(io::ReadStream, dest::AbstractVector{UInt8}, n)
    eof(io) && return 0
    if n == typemax(Int)
        bytes_read = 0
        while !eof(io)
            bytes_to_read = 128 * 1024
            bytes_read + bytes_to_read > length(dest) && resize!(dest, bytes_read + bytes_to_read)
            bytes_read += GC.@preserve dest _unsafe_read(io, pointer(dest, bytes_read+1), bytes_to_read)
        end
        resize!(dest, bytes_read)
        return bytes_read
    else
        bytes_to_read = n == typemax(Int) ? 64 * 1024 : Int(n)
        bytes_to_read > length(dest) && resize!(dest, bytes_to_read)
        bytes_read = GC.@preserve dest _unsafe_read(io, pointer(dest), bytes_to_read)
        return bytes_read
    end
end

function Base.unsafe_read(io::ReadStream, p::Ptr{UInt8}, nb::UInt)
    if eof(io)
        nb > 0 && throw(EOFError())
        return nothing
    end
    bytes_read = _unsafe_read(io, p, Int(nb))
    eof(io) && nb > bytes_read && throw(EOFError())
    return nothing
end

# TranscodingStreams.jl are calling this method when Base.bytesavailable is zero
# to trigger buffer refill
function Base.read(io::ReadStream, ::Type{UInt8})
    eof(io) && throw(EOFError())
    buf = zeros(UInt8, 1)
    n = _unsafe_read(io, pointer(buf), 1)
    n < 1 && throw(EOFError())
    @inbounds b = buf[1]
    return b
end

function _forward(to::IO, from::IO)
    buf = Vector{UInt8}(undef, 64 * 1024)
    n = 0
    while !eof(from)
        bytes_read = readbytes!(from, buf, 64 * 1024)
        bytes_written = 0
        while bytes_written < bytes_read
            bytes_written += write(to, buf[bytes_written+1:bytes_read])
        end
        n += bytes_written
    end

    return n
end

function Base.write(to::IO, from::ReadStream)
    return _forward(to, from)
end

"""
    get_object_stream(path, conf; size_hint, decompress) -> ReadStream

Send a get request to the object store returning a stream of object data.

# Arguments
- `path::String`: The location of the data to fetch.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Keyword
- `size_hint::Int`: (Optional) Expected size of the object (optimization for small objects).
- `decompress::Option{String}`: (Optional) Compression algorithm to decode the response stream (supports gzip, deflate, zlib or zstd)

# Returns
- `stream::ReadStream`: The stream of object data chunks.

# Throws
- `GetException`: If the request fails for any reason.
"""
function get_object_stream(path::String, conf::AbstractConfig; size_hint::Int=0, decompress::String="")
    response = ReadStreamResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    hint = convert(UInt64, size_hint)
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.get_stream(
                path::Cstring,
                hint::Culonglong,
                decompress::Cstring,
                config::Ref{Config},
                response::Ref{ReadStreamResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        # No need to destroy_read_stream in case of errors here
        @throw_on_error(response, "get_stream", GetException)

        return ReadStream(
            response.stream,
            convert(Int, response.object_size),
            0,
            false,
            nothing
        )
    end
end

function _unsafe_read(stream::ReadStream, dest::Ptr{UInt8}, bytes_to_read::Int)
    if stream.ended
        return nothing
    end
    if !isnothing(stream.error)
        throw("stream stopped by prevoius error: $(stream.error)")
    end

    response = ReadResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    preserve_task(ct)
    GC.@preserve stream dest response event try
        result = @ccall rust_lib.read_from_stream(
            stream.ptr::Ptr{Cvoid},
            dest::Ptr{UInt8},
            bytes_to_read::Culonglong,
            bytes_to_read::Culonglong,
            response::Ref{ReadResponseFFI},
            handle::Ptr{Cvoid}
        )::Cint

        wait_or_cancel(event, response)
    finally
        unpreserve_task(ct)
    end

    try
        @throw_on_error(response, "read_from_stream", GetException)
    catch e
        stream_error!(stream, e.msg)
        rethrow()
    end

    if response.length > 0
        stream.bytes_read += response.length
        if response.eof == 0
            return convert(Int, response.length)
        else
            stream_end!(stream)
            return convert(Int, response.length)
        end
    else
        stream_end!(stream)
        return nothing
    end
end

"""
    finish!(stream::ReadStream) -> Bool

Finishes the stream reclaiming resources.

This function is not thread-safe.

# Arguments
- `stream::ReadStream`: The stream of object data.

# Returns
- `was_running::Bool`: Indicates if the stream was running when `finish!` was called.
"""
function finish!(stream::ReadStream)
    if !Base.isopen(stream)
        return false
    end
    stream_end!(stream)
    return true
end

mutable struct WriteResponseFFI
    result::Cint
    length::Culonglong
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    WriteResponseFFI() = new(-1, 0, C_NULL, C_NULL)
end

mutable struct WriteStreamResponseFFI
    result::Cint
    stream::Ptr{Nothing}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    WriteStreamResponseFFI() = new(-1, C_NULL, C_NULL, C_NULL)
end

"""
    WriteStream


Opaque IO sink of object data.

It is necessary to call `shutdown!` to ensure data is persisted, or `cancel!` if the stream is to be discarded.

"""
mutable struct WriteStream <: IO
    ptr::Ptr{Nothing}
    bytes_written::Int
    destroyed::Bool
    error::Option{String}
end

"""
    put_object_stream(path, conf; compress) -> WriteStream

Send a put request to the object store returning a stream to write data into.

# Arguments
- `path::String`: The location where to write the object.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.

# Keyword
- `compress::Option{String}`: (Optional) Compression algorithm to encode the stream (supports gzip, deflate, zlib or zstd)

# Returns
- `stream::WriteStream`: The stream where to write object data.

# Throws
- `PutException`: If the request fails for any reason.
"""
function put_object_stream(path::String, conf::AbstractConfig; compress::String="")
    response = WriteStreamResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.put_stream(
                path::Cstring,
                compress::Cstring,
                config::Ref{Config},
                response::Ref{WriteStreamResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        # No need to destroy_write_stream in case of errors here
        @throw_on_error(response, "put_stream", PutException)

        return WriteStream(
            response.stream,
            0,
            false,
            nothing
        )
    end
end

"""
    cancel!(stream::WriteStream) -> Bool

Cancels the stream reclaiming resources.

No partial writes will be observed.

This function is not thread-safe.

# Arguments
- `stream::WriteStream`: The writeable stream to be canceled.

# Returns
- `was_writeable::Bool`: Indicates if the stream was writeable when `cancel!` was called.
"""
function cancel!(stream::WriteStream)
    if !Base.isopen(stream)
        return false
    end
    stream_destroy(stream)
    return true
end

"""
    shutdown!(stream::WriteStream) -> Bool

Shuts down the stream ensuring the data is persisted.

On failure partial writes will NOT be observed.

This function is not thread-safe.

# Arguments
- `stream::WriteStream`: The writeable stream to be shutdown.
"""
function shutdown!(stream::WriteStream)
    if !isnothing(stream.error)
        throw(PutException("Tried to shutdown a stream in error state, previous error: $(stream.error)"))
    end
    if stream.destroyed
        throw(PutException("Tried to shutdown a destroyed stream (from a previous `cancel!` or `shutdown!`)"))
    end

    response = WriteResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    GC.@preserve stream response event try
        preserve_task(ct)
        result = @ccall rust_lib.shutdown_write_stream(
            stream.ptr::Ptr{Cvoid},
            response::Ref{WriteResponseFFI},
            handle::Ptr{Cvoid}
        )::Cint

        @assert result == 0

        wait_or_cancel(event, response)
    finally
        unpreserve_task(ct)
    end

    try
        @throw_on_error(response, "shutdown_write_stream", PutException)
    catch e
        stream_error!(stream, e.msg)
        rethrow()
    end

    if response.result == 0
        stream_destroy(stream)
        return nothing
    else
        @assert false "unreachable"
    end
end

Base.isopen(io::WriteStream) = !io.destroyed && isnothing(io.error)
Base.iswritable(io::WriteStream) = true
function Base.close(io::WriteStream)
    shutdown!(io)
    return nothing
end
function Base.flush(stream::WriteStream)
    _unsafe_write(stream, convert(Ptr{UInt8}, C_NULL), 0; flush=true)
    return nothing
end
function Base.unsafe_write(stream::WriteStream, input::Ptr{UInt8}, nbytes::Int)
    _unsafe_write(stream, input, nbytes)
    return nothing
end
function Base.write(io::WriteStream, bytes::Vector{UInt8})
    return _unsafe_write(io, pointer(bytes), length(bytes))
end
function Base.write(to::WriteStream, from::IO)
    return _forward(to, from)
end
function Base.write(to::WriteStream, from::ReadStream)
    return _forward(to, from)
end

function stream_destroy(io::WriteStream)
    @assert Base.isopen(io)
    io.destroyed = true
    @ccall rust_lib.destroy_write_stream(io.ptr::Ptr{Nothing})::Cint
end

function stream_error!(io::WriteStream, err::String)
    @assert Base.isopen(io)
    io.error = err
    @ccall rust_lib.destroy_write_stream(io.ptr::Ptr{Nothing})::Cint
end

function _unsafe_write(stream::WriteStream, input::Ptr{UInt8}, nbytes::Int; flush=false)
    if !isnothing(stream.error)
        throw(PutException("Tried to write to a stream in error state, previous error: $(stream.error)"))
    end
    if stream.destroyed
        throw(PutException("Tried to write to a destroyed stream (from a previous `cancel!` or `shutdown!`)"))
    end

    response = WriteResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    GC.@preserve stream response event try
        preserve_task(ct)
        result = @ccall rust_lib.write_to_stream(
            stream.ptr::Ptr{Cvoid},
            input::Ptr{UInt8},
            nbytes::Culonglong,
            flush::Cuchar,
            response::Ref{WriteResponseFFI},
            handle::Ptr{Cvoid}
        )::Cint

        @assert result == 0

        wait_or_cancel(event, response)
    finally
        unpreserve_task(ct)
    end

    try
        @throw_on_error(response, "write_to_stream", PutException)
    catch e
        stream_error!(stream, e.msg)
        rethrow()
    end

    @assert response.result == 0

    stream.bytes_written += response.length
    return Int(response.length)
end

# List operations

"""
    function max_entries_per_chunk()::Int

Return the maximum number of entries a listing stream chunk can hold.
This is kept in sync manually with the Rust care for now, it should later be re-expoted.
"""
max_entries_per_chunk() = 1000

struct ListEntryFFI
    location::Cstring
    last_modified::Culonglong
    size::Culonglong
    e_tag::Cstring
    version::Cstring
end

struct ListEntry
    location::String
    last_modified::Int
    size::Int
    e_tag::Option{String}
    version::Option{String}
end

function convert_list_entry(entry::ListEntryFFI)
    return ListEntry(
        unsafe_string(entry.location),
        convert(Int, entry.last_modified),
        convert(Int, entry.size),
        entry.e_tag != C_NULL ? unsafe_string(entry.e_tag) : nothing,
        entry.version != C_NULL ? unsafe_string(entry.version) : nothing
    )
end

mutable struct ListResponseFFI
    result::Cint
    entries::Ptr{ListEntryFFI}
    entry_count::Culonglong
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ListResponseFFI() = new(-1, C_NULL, 0, C_NULL, C_NULL)
end

"""
    list_objects(prefix, conf; offset) -> Vector{ListEntry}
Send a list request to the object store.
This buffers all entries in memory. For large (or unknown) object counts use `list_objects_stream`.
# Arguments
- `prefix::String`: Only objects with this prefix will be returned.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.
# Keyword Arguments
- `offset::Option{String}`: (Optional) Start listing after this offset
# Returns
- `entries::Vector{ListEntry}`: The array with metadata for each object in the prefix.
  Returns an empty array if no objects match.
# Throws
- `ListException`: If the request fails for any reason.
"""
function list_objects(prefix::String, conf::AbstractConfig; offset::Option{String} = nothing)
    response = ListResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    c_offset = if isnothing(offset)
        C_NULL
    else
        offset
    end
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.list(
                prefix::Cstring,
                c_offset::Cstring,
                config::Ref{Config},
                response::Ref{ListResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        # No need to destroy_list_response in case of errors here
        @throw_on_error(response, "list", ListException)

        entries = if response.entry_count > 0
            raw_entries = unsafe_wrap(Array, response.entries, response.entry_count)
            vector = map(convert_list_entry, raw_entries)
            @ccall rust_lib.destroy_list_entries(
                response.entries::Ptr{ListEntryFFI},
                response.entry_count::Culonglong
            )::Cint
            vector
        else
            Vector{ListEntry}[]
        end

        return entries
    end
end

mutable struct ListStreamResponseFFI
    result::Cint
    stream::Ptr{Nothing}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    ListStreamResponseFFI() = new(-1, C_NULL, C_NULL, C_NULL)
end

"""
    ListStream
Opaque stream of metadata list chunks (Vector{ListEntry}).
Use `next_chunk!` repeatedly to fetch data. An empty chunk indicates end of stream.
The stream stops if an error occours, any following calls to `next_chunk!` will repeat the same error.
It is necessary to `finish!` the stream if it is not run to completion.
"""
mutable struct ListStream
    ptr::Ptr{Nothing}
    ended::Bool
    error::Option{String}
end

function stream_end!(stream::ListStream)
    @assert (!stream.ended && isnothing(stream.error))
    stream.ended = true
    @ccall rust_lib.destroy_list_stream(stream.ptr::Ptr{Nothing})::Cint
end

function stream_error!(stream::ListStream, err::String)
    @assert (!stream.ended && isnothing(stream.error))
    stream.error = err
    @ccall rust_lib.destroy_list_stream(stream.ptr::Ptr{Nothing})::Cint
end

"""
    list_objects_stream(prefix, conf) -> ListStream
Send a list request to the object store returning a stream of entry chunks.
# Arguments
- `prefix::String`: Only objects with this prefix will be returned.
- `conf::AbstractConfig`: The configuration to use for the request.
  It includes credentials and other client options.
# Keyword Arguments
- `offset::Option{String}`: (Optional) Start listing after this offset
# Returns
- `stream::ListStream`: The stream of object metadata chunks.
# Throws
- `ListException`: If the request fails for any reason.
"""
function list_objects_stream(prefix::String, conf::AbstractConfig; offset::Option{String} = nothing)
    response = ListStreamResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    c_offset = if isnothing(offset)
        C_NULL
    else
        offset
    end
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.list_stream(
                prefix::Cstring,
                c_offset::Cstring,
                config::Ref{Config},
                response::Ref{ListStreamResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        # No need to destroy_list_stream in case of errors here
        @throw_on_error(response, "list_stream", ListException)

        return ListStream(response.stream, false, nothing)
    end
end

"""
    next_chunk!(stream) -> Option{Vector{ListEntry}}
Fetch the next chunk from a ListStream.
If the returned entries are the last in the stream, `stream.ended` will be set to true.
An empty chunk indicates end of stream too.
After an error any following calls will replay the error.
# Arguments
- `stream::ListStream`: The stream of object metadata list chunks.
# Returns
- `entries::Vector{ListEntry}`: The array with metadata for each object in the prefix.
  Resturns and empty array if no objects match or the stream is over.
# Throws
- `ListException`: If the request fails for any reason.
"""
function next_chunk!(stream::ListStream)
    if !isnothing(stream.error)
        throw(PutException("Tried to fetch next chunk from a stream in error state, previous error: $(stream.error)"))
    end
    if stream.ended
        return nothing
    end

    response = ListResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    GC.@preserve stream response event try
        preserve_task(ct)
        result = @ccall rust_lib.next_list_stream_chunk(
            stream.ptr::Ptr{Cvoid},
            response::Ref{ListResponseFFI},
            handle::Ptr{Cvoid}
        )::Cint

        @assert result == 0

        wait_or_cancel(event, response)
    finally
        unpreserve_task(ct)
    end

    try
        @throw_on_error(response, "next_list_stream_chunk", ListException)
    catch e
        stream_error!(stream, e.msg)
        rethrow()
    end

    @assert response.result == 0

    # To avoid calling `next_chunk!` again on a practically ended stream, we mark
    # the stream as ended if the response has less entries than the chunk maximum.
    # This is safe to do because the Rust backend always fill the chunk to the maximum
    # unless the underlying stream is drained.
    if response.entry_count < max_entries_per_chunk()
        stream_end!(stream)
    end

    if response.entry_count > 0
        raw_entries = unsafe_wrap(Array, response.entries, response.entry_count)
        vector = map(convert_list_entry, raw_entries)
        @ccall rust_lib.destroy_list_entries(
            response.entries::Ptr{ListEntryFFI},
            response.entry_count::Culonglong
        )::Cint
        return vector
    else
        return nothing
    end
end


"""
    finish!(stream) -> Bool
Finishes the stream reclaiming resources.
This function is not thread-safe.
# Arguments
- `stream::ListStream`: The stream of object metadata list chunks.
# Returns
- `was_running::Bool`: Indicates if the stream was running when `finish!` was called.
"""
function finish!(stream::ListStream)
    if stream.ended || !isnothing(stream.error)
        return false
    end
    stream_end!(stream)
    return true
end

mutable struct StageInfoResponseFFI
    result::Cint
    stage_info::Ptr{Cchar}
    error_message::Ptr{Cchar}
    context::Ptr{Cvoid}

    StageInfoResponseFFI() = new(-1, C_NULL, C_NULL, C_NULL)
end

function current_stage_info(conf::AbstractConfig)
    response = StageInfoResponseFFI()
    ct = current_task()
    event = Base.Event()
    handle = pointer_from_objref(event)
    config = into_config(conf)
    while true
        preserve_task(ct)
        result = GC.@preserve config response event try
            result = @ccall rust_lib.current_stage_info(
                config::Ref{Config},
                response::Ref{StageInfoResponseFFI},
                handle::Ptr{Cvoid}
            )::Cint

            wait_or_cancel(event, response)

            result
        finally
            unpreserve_task(ct)
        end

        if result == 2
            # backoff
            sleep(0.01)
            continue
        end

        # No need to destroy_write_stream in case of errors here
        @throw_on_error(response, "current_stage_info", GetException)

        info_string = unsafe_string(response.stage_info)
        @ccall rust_lib.destroy_cstring(response.stage_info::Ptr{Cchar})::Cint

        stage_info = JSON3.read(info_string, Dict{String, String})
        return stage_info
    end
end

struct Metrics
    live_bytes::Int64
end

function current_metrics()
    return @ccall rust_lib.current_metrics()::Metrics
end

module Test
include("mock_server.jl")
end # Test module

end # RustyObjectStore module
