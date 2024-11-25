using CloudBase: CloudBase, CloudCredentials, AbstractStore
using CloudBase: AWSCredentials, AWS
using CloudBase: AzureCredentials, Azure
using JSON3, HTTP, Sockets, Base64
using RustyObjectStore: SnowflakeConfig, ClientOptions
using Base: UUID

export SFGatewayMock, start, with

struct SFConfig
    account::String
    database::String
    default_schema::String
end

struct Stage
    database::String
    schema::String
    name::String
end

mutable struct SFGatewayMock
    credentials::CloudCredentials
    store::AbstractStore
    opts::ClientOptions
    config::SFConfig
    allowed_stages::Vector{Stage}
    encrypted::Bool
    keys_lock::ReentrantLock
    next_key_id::Int
    keys::Dict{String, String}
end


function to_stage(stage::AbstractString, config::SFConfig)
    parts = split(stage, ".")
    if length(parts) == 1
        return Stage(uppercase(config.database), uppercase(config.default_schema), uppercase(parts[1]))
    elseif length(parts) == 2
        return Stage(uppercase(config.database), uppercase(parts[1]), uppercase(parts[2]))
    elseif length(parts) == 3
        return Stage(uppercase(parts[1]), uppercase(parts[2]), uppercase(parts[3]))
    else
        error("Invalid stage spec")
    end
end

fqsn(s::Stage) = "$(s.database).$(s.schema).$(s.name)"
stage_uuid(s::Stage) = UUID((hash(fqsn(s)), hash(fqsn(s))))
stage_path(s::Stage) = "stages/$(stage_uuid(s))/"

function SFGatewayMock(
        credentials::CloudCredentials,
        store::AbstractStore,
        encrypted::Bool;
        opts=ClientOptions(),
        default_schema::String="testschema",
        allowed_stages::Vector{String}=["teststage" * string(rand(UInt64), base=16)]
    )
    config = SFConfig(
        "testaccount",
        "testdatabase",
        default_schema
    )
    allowed_stages_parsed = map(s -> to_stage(s, config), allowed_stages)
    SFGatewayMock(
        credentials,
        store,
        opts,
        config,
        allowed_stages_parsed,
        encrypted,
        ReentrantLock(),
        1,
        Dict{String, String}()
    )
end

function authorized(request::HTTP.Request)
    return HTTP.header(request, "Authorization") == "Snowflake Token=\"dummy-token\""
end

function unauthorized_response()
    return HTTP.Response(401, "Invalid token")
end

function error_json_response(msg::String; code::String="1234")
    response_data = Dict(
        "data" => Dict(
            "queryId" => "dummy-query-id"
        ),
        "code" => code,
        "message" => msg,
        "success" => false
    )
    return HTTP.Response(200, JSON3.write(response_data))
end

function construct_stage_info(credentials::AWSCredentials, store::AWS.Bucket, path::String, encrypted::Bool)
    m = match(r"(https?://.*?)/", store.baseurl)
    @assert !isnothing(m)
    test_endpoint = m.captures[1]

    Dict(
        "locationType" => "S3",
        "location" => joinpath(store.name, "xox50000-s", path),
        "path" => path,
        "region" => "us-east-1",
        "storageAccount" => nothing,
        "isClientSideEncrypted" => encrypted,
        "ciphers" => encrypted ? "AES_CBC" : nothing,
        "creds" => Dict(
            "AWS_KEY_ID" => credentials.access_key_id,
            "AWS_SECRET_KEY" => credentials.secret_access_key,
            "AWS_TOKEN" => credentials.session_token
        ),
        "useS3RegionalUrl" => false,
        "endPoint" => "us-east-1.s3.amazonaws.com",
        "testEndpoint" => test_endpoint
    )
end

function construct_stage_info(credentials::AzureCredentials, store::Azure.Container, encrypted::Bool)
    ok, _service, test_endpoint, account, container, _path =
        CloudBase.parseAzureAccountContainerBlob(rstrip(store.baseurl, '/'); parseLocal=true)
    ok || error("failed to parse Azurite baseurl")

    Dict(
        "locationType" => "AZURE",
        "location" => container * "/",
        "path" => container * "/",
        "region" => "westus2",
        "storageAccount" => account,
        "isClientSideEncrypted" => encrypted,
        "ciphers" => encrypted ? "AES_CBC" : nothing,
        "creds" => Dict(
            "AZURE_SAS_TOKEN" => "dummy-token",
        ),
        "useS3RegionalUrl" => false,
        "endPoint" => "blob.core.windows.net",
        "testEndpoint" => test_endpoint,
    )
end

function next_id_and_key(gw::SFGatewayMock)
    @lock gw.keys_lock begin
        key_id = gw.next_key_id
        gw.next_key_id += 1
        key = base64encode(rand(UInt8, 16))
        push!(gw.keys, string(key_id) => key)
        return key_id, key
    end
end

function find_key_by_id(gw::SFGatewayMock, id::String)
    @lock gw.keys_lock begin
        return get(gw.keys, id, nothing)
    end
end

# Returns a SnowflakeConfig and a server instance.
# The config can be used to perform operations against
# a simulated Snowflake stage backed by a Minio instance.
function start(gw::SFGatewayMock)
    (port, tcp_server) = Sockets.listenany(8080)
    http_server = HTTP.serve!(tcp_server) do request::HTTP.Request
        if request.method == "POST" && startswith(request.target, "/session/heartbeat")
            # Heartbeat
            authorized(request) || return unauthorized_response()
            return HTTP.Response(200, "Pong")
        elseif request.method == "POST" && startswith(request.target, "/session/token-request")
            # Token Renewal
            authorized(request) || return unauthorized_response()
            object = JSON3.read(request.body)
            if get(object, "oldSessionToken", nothing) != "dummy-token"
                return error_json_response("Invalid session token")
            end

            response_data = Dict(
                "data" => Dict(
                    "sessionToken" => "dummy-token",
                    "validityInSecondsST" => 3600,
                    "masterToken" => "dummy-master-token",
                    "validityInSecondsMT" => 3600
                ),
                "success" => true
            )
            return HTTP.Response(200, JSON3.write(response_data))
        elseif request.method == "POST" && startswith(request.target, "/session/v1/login-request")
            # Login
            response_data = Dict(
                "data" => Dict(
                    "token" => "dummy-token",
                    "validityInSeconds" => 3600,
                    "masterToken" => "dummy-master-token",
                    "masterValidityInSeconds" => 3600
                ),
                "success" => true
            )
            return HTTP.Response(200, JSON3.write(response_data))
        elseif request.method == "POST" && startswith(request.target, "/queries/v1/query-request")
            # Query
            authorized(request) || return unauthorized_response()
            object = JSON3.read(request.body)
            sql = get(object, "sqlText", nothing)
            if isnothing(sql) || !isa(sql, String)
                return error_json_response("Missing sql query text")
            end

            sql = strip(sql)

            if startswith(sql, "PUT")
                m = match(r"PUT\s+?file://.*?\s+?@(.+?)(\s|$)", sql)
                if isnothing(m)
                    return error_json_response("Missing stage name or file path")
                end

                stage = try
                    to_stage(m.captures[1], gw.config)
                catch e
                    return error_json_response("$(e)")
                end

                if !(stage in gw.allowed_stages)
                    return error_json_response("Stage not found")
                end

                encryption_material = if gw.encrypted
                    # generate new key
                    key_id, key = next_id_and_key(gw)
                    Dict(
                        "queryStageMasterKey" => key,
                        "queryId" => string(key_id),
                        "smkId" => key_id
                    )
                else
                    nothing
                end


                stage_info = if isa(gw.credentials, AWSCredentials) && isa(gw.store, AWS.Bucket)
                    construct_stage_info(gw.credentials, gw.store, stage_path(stage), gw.encrypted)
                elseif isa(gw.credentials, AzureCredentials) && isa(gw.store, Azure.Container)
                    construct_stage_info(gw.credentials, gw.store, gw.encrypted)
                else
                    error("unimplemented")
                end

                # PUT Query
                response_data = Dict(
                    "data" => Dict(
                        "queryId" => "dummy-query-id",
                        "encryptionMaterial" => encryption_material,
                        "stageInfo" => stage_info
                    ),
                    "success" => true
                )
                return HTTP.Response(200, JSON3.write(response_data))
            elseif startswith(sql, "GET")
                # GET Query
                m = match(r"GET\s+?@(.+?)/(.+?)\s", sql)
                if isnothing(m)
                    return error_json_response("Missing stage name or file path")
                end

                stage = try
                    to_stage(m.captures[1], gw.config)
                catch e
                    return error_json_response("$(e)")
                end

                if !(stage in gw.allowed_stages)
                    return error_json_response("Stage not found")
                end

                path = m.captures[2]

                stage_info = if isa(gw.credentials, AWSCredentials) && isa(gw.store, AWS.Bucket)
                    construct_stage_info(gw.credentials, gw.store, stage_path(stage), gw.encrypted)
                elseif isa(gw.credentials, AzureCredentials) && isa(gw.store, Azure.Container)
                    construct_stage_info(gw.credentials, gw.store, gw.encrypted)
                else
                    error("unimplemented")
                end

                encryption_material = if gw.encrypted
                    # fetch key id from blob meta and return key
                    headers, metadata_key = if isa(gw.credentials, AWSCredentials)
                        response = AWS.head(
                            stage_info["testEndpoint"] * "/" * stage_info["location"] * path;
                            service="s3", region="us-east-1", credentials=gw.credentials
                        )
                        response.headers, "x-amz-meta-x-amz-matdesc"
                    elseif isa(gw.credentials, AzureCredentials)
                        response = Azure.head(
                            stage_info["testEndpoint"] * "/" * stage_info["storageAccount"] * "/" * stage_info["location"] * path;
                            service="blob", region="westus2", credentials=gw.credentials
                        )
                        response.headers, "x-ms-meta-matdesc"
                    else
                        error("unknown credentials type: $(typeof(gw.credentials))")
                    end
                    pos = findfirst(x -> x[1] == metadata_key, headers)
                    matdesc = JSON3.read(headers[pos][2])
                    key_id = matdesc["queryId"]
                    key = find_key_by_id(gw, key_id)
                    Dict(
                        "queryStageMasterKey" => key,
                        "queryId" => key_id,
                        "smkId" => parse(Int, key_id)
                    )
                else
                    nothing
                end

                response_data = Dict(
                    "data" => Dict(
                        "queryId" => "dummy-query-id",
                        "src_locations" => [path],
                        "encryptionMaterial" => [encryption_material],
                        "stageInfo" => stage_info
                    ),
                    "success" => true
                )
                return HTTP.Response(200, JSON3.write(response_data))
            else
                return error_json_response("Unsupported query")
            end
        else
            return HTTP.Response(404, "Not Found")
        end
    end

    master_path, fileio = mktemp()
    write(fileio, "dummy-file-master-token")
    sfconfig = SnowflakeConfig(
        stage=fqsn(gw.allowed_stages[1]),
        account=gw.config.account,
        database=gw.config.database,
        schema=gw.allowed_stages[1].schema,
        endpoint="http://127.0.0.1:$(port)",
        master_token_path=master_path,
        opts=gw.opts
    )
    return sfconfig, http_server
end

function with(f::Function, gw::SFGatewayMock)
    config, server = start(gw)
    try
        f(config)
    finally
        HTTP.forceclose(server)
        rm(config.master_token_path)
    end
end
