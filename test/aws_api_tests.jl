@testitem "AwsConfig" begin
    # access key is obscured when printing
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :aws_access_key_id => \"*****\", :region => \"a\", :bucket_name => \"b\"))"

    # session token is obscured when printing
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        session_token="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :aws_access_key_id => \"*****\", :aws_session_token => \"*****\", :region => \"a\", :bucket_name => \"b\"))"

    # host is supported
    @test repr(AwsConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        host="d"
    )) == "RustyObjectStore.AwsConfig(Config(:aws_secret_access_key => \"*****\", :url => \"s3://b/\", :aws_access_key_id => \"*****\", :region => \"a\", :minio_host => \"d\", :bucket_name => \"b\"))"
end
