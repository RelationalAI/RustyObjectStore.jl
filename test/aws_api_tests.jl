@testitem "AWSConfig" begin
    # access key is obscured when printing
    @test repr(AWSConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d"
    )) == "AWSConfig(region=\"a\", bucket_name=\"b\", access_key_id=*****, secret_access_key=*****, opts=ClientOptions())"

    # session token is obscured when printing
    @test repr(AWSConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        session_token="d"
    )) == "AWSConfig(region=\"a\", bucket_name=\"b\", access_key_id=*****, secret_access_key=*****, session_token=*****, opts=ClientOptions())"

    # host is supported
    @test repr(AWSConfig(;
        region="a",
        bucket_name="b",
        access_key_id="c",
        secret_access_key="d",
        host="d"
    )) == "AWSConfig(region=\"a\", bucket_name=\"b\", access_key_id=*****, secret_access_key=*****, host=\"d\", opts=ClientOptions())"
end
