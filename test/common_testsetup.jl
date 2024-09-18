@testsetup module InitializeObjectStore
    using RustyObjectStore
    test_config = StaticConfig(
        n_threads=0,
        cache_capacity=20,
        cache_ttl_secs=30 * 60,
        cache_tti_secs=5 * 60,
        multipart_put_threshold=8 * 1024 * 1024,
        multipart_get_threshold=8 * 1024 * 1024,
        multipart_get_part_size=8 * 1024 * 1024,
        concurrency_limit=512
    )
    init_object_store(test_config)
end

@testsetup module SnowflakeMock
    using CloudBase: CloudCredentials, AWSCredentials, AbstractStore, AWS
    using JSON3, HTTP, Sockets, Base64
    using RustyObjectStore: SnowflakeConfig, ClientOptions

    export SFGatewayMock, start, with

    struct SFConfig
        account::String
        database::String
        schema::String
        stage::String
    end

    mutable struct SFGatewayMock
        credentials::CloudCredentials
        store::AbstractStore
        opts::ClientOptions
        config::SFConfig
        encrypted::Bool
        keys_lock::ReentrantLock
        next_key_id::Int
        keys::Dict{String, String}
    end

    function SFGatewayMock(credentials::CloudCredentials, store::AbstractStore, encrypted::Bool; opts=ClientOptions())
        stage_name = "teststage" * string(rand(UInt64), base=16)
        config = SFConfig(
            "testaccount",
            "testdatabase",
            "testschema",
            stage_name
        )
        SFGatewayMock(
            credentials,
            store,
            opts,
            config,
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
        @show test_endpoint

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

                    stage_name = m.captures[1]

                    if stage_name != gw.config.stage
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


                    stage_path = "stages/a6688b33-acb4-44ed-bd46-30ff12238c2a/"
                    stage_info = if isa(gw.credentials, AWSCredentials) && isa(gw.store, AWS.Bucket)
                        construct_stage_info(gw.credentials, gw.store, stage_path, gw.encrypted)
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

                    stage_name = m.captures[1]
                    path = m.captures[2]

                    if stage_name != gw.config.stage
                        return error_json_response("Stage not found")
                    end

                    stage_path = "stages/a6688b33-acb4-44ed-bd46-30ff12238c2a/"
                    stage_info = if isa(gw.credentials, AWSCredentials) && isa(gw.store, AWS.Bucket)
                        construct_stage_info(gw.credentials, gw.store, stage_path, gw.encrypted)
                    else
                        error("unimplemented")
                    end

                    encryption_material = if gw.encrypted
                        # fetch key id from s3 meta and return key
                        response = AWS.head(
                            stage_info["testEndpoint"] * "/" * stage_info["location"] * path;
                            service="s3", region="us-east-1", credentials=gw.credentials
                        )
                        pos = findfirst(x -> x[1] == "x-amz-meta-x-amz-matdesc", response.headers)
                        matdesc = JSON3.read(response.headers[pos][2])
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
            stage=gw.config.stage,
            account=gw.config.account,
            database=gw.config.database,
            schema=gw.config.schema,
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
end
