abstract type Builder end

function add_entry(b::Builder, key::String, value::String) end

struct ConfigBuilder <: Builder
    params::Dict{String, String}
    function ConfigBuilder()
        new(Dict())
    end
end

struct AzureConfigBuilder <: Builder
    params::Dict{String, String}

    function AzureConfigBuilder(params::Dict{String, String} = Dict())
        return new(params)
    end
end

struct AwsConfigBuilder <: Builder
    params::Dict{String, String}

    function AwsConfigBuilder(params::Dict{String, String} = Dict())
        return new(params)
    end
end

add_entry(b::ConfigBuilder, key::String, value::String) = b.params[key] = value
add_entry(b::AzureConfigBuilder, key::String, value::String) =b.params[key] = value
add_entry(b::AwsConfigBuilder, key::String, value::String) =b.params[key] = value


function with_request_timeout_secs(timeout_secs::Int)
    return function (b::T) where {T <: Builder}
        add_entry(b, "timeout", "$(string(timeout_secs))s")
        return b
    end
end

function with_connect_timeout_secs(connect_timeout_secs::Int)
    return function (b::T) where {T <: Builder}
        add_entry(b, "connect_timeout", "$(string(connect_timeout_secs))s")
        return b
    end
end

function with_max_retries(max_retries::Int)
    return function (b::T) where {T <: Builder}
        add_entry(b, "max_retries", string(max_retries))
        return b
    end
end

function with_retry_timeout_secs(retry_timeout_secs::Int)
    return function (b::T) where {T <: Builder}
        add_entry(b, "retry_timeout_secs", string(retry_timeout_secs))
        return b
    end
end

function azure(b::ConfigBuilder)
    return AzureConfigBuilder(b.params)
end

function with_container_name(container_name::String)
    return function (b::AzureConfigBuilder)
        b.params["azure_container_name"] = container_name
        return b
    end
end

function with_storage_account_name(account_name::String)
    return function (b::AzureConfigBuilder)
        b.params["azure_storage_account_name"] = account_name
        return b
    end
end

function with_storage_account_key(account_key::String)
    return function (b::AzureConfigBuilder)
        b.params["azure_storage_account_key"] = account_key
        return b
    end
end

function with_storage_sas_token(sas_token::String)
    return function (b::AzureConfigBuilder)
        b.params["azure_storage_sas_token"] = sas_token
        return b
    end
end

function build(b::AzureConfigBuilder)
    dict = b.params
    haskey(dict, "azure_container_name") || error("Missing container_name")
    haskey(dict, "azure_storage_account_name") || error("Missing storage_account_name")
    (
        haskey(dict, "azure_storage_account_key")
        ⊻ haskey(dict, "azure_storage_sas_token")
    ) || error("Must provide either a storage_account_key or a storage_sas_token")
    @show b
    return Config("az://$(dict["azure_container_name"])/", dict)
end

function aws(b::ConfigBuilder)
    return AwsConfigBuilder(b.params)
end

function with_bucket_name(bucket_name::String)
    return function (b::AwsConfigBuilder)
        b.params["aws_bucket_name"] = bucket_name
        return b
    end
end

function with_region(region::String)
    return function (b::AwsConfigBuilder)
        b.params["aws_region"] = region
        return b
    end
end

function with_access_key(access_key_id::String, secret_access_key::String)
    return function (b::AwsConfigBuilder)
        b.params["aws_access_key_id"] = access_key_id
        b.params["aws_secret_access_key"] = secret_access_key
        return b
    end
end

function with_sts_token(access_key_id::String, secret_access_key::String, session_token::String)
    return function (b::AwsConfigBuilder)
        b.params["aws_access_key_id"] = access_key_id
        b.params["aws_secret_access_key"] = secret_access_key
        b.params["aws_session_token"] = session_token
        return b
    end
end

function build(b::AwsConfigBuilder)
    dict = b.params
    haskey(dict, "aws_bucket_name") || error("Missing bucket_name")
    haskey(dict, "aws_region") || error("Missing region")
    @show b
    return Config("s3://$(dict["aws_bucket_name"])/", dict)
end

# azure_config = ConfigBuilder() |>
#     with_request_timeout_secs(30) |>
#     with_connect_timeout_secs(5) |>
#     with_max_retries(15) |>
#     with_retry_timeout_secs(150) |>
#     azure |>
#     with_storage_account_name("aguedes") |>
#     with_container_name("test-blob") |>
#     with_storage_account_key("test") |>
#     build
#
# aws_config = ConfigBuilder() |>
#     with_request_timeout_secs(30) |>
#     with_connect_timeout_secs(5) |>
#     with_max_retries(15) |>
#     with_retry_timeout_secs(150) |>
#     aws |>
#     with_region("us-east-1") |>
#     with_bucket_name("sf") |>
#     with_access_key("id", "secret") |>
#     with_sts_token("id", "secret", "session") |>
#     build
