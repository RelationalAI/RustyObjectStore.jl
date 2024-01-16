@testitem "AwsConfig" begin
    # access key is obscured when printing
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :max_retries => \"10\", :aws_access_key_id => \"*****\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :region => \"a\", :bucket_name => \"b\", :connect_timeout => \"5s\"))"

    # session token is obscured when printing
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        session_token="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :max_retries => \"10\", :aws_access_key_id => \"*****\", :aws_session_token => \"*****\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :region => \"a\", :bucket_name => \"b\", :connect_timeout => \"5s\"))"

    # host is supported
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        host="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :max_retries => \"10\", :aws_access_key_id => \"*****\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :region => \"a\", :minio_host => \"d\", :connect_timeout => \"5s\", :bucket_name => \"b\"))"
end
