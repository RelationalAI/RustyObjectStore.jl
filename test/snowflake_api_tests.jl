@testitem "SnowflakeConfig" begin
    # basic case
    @test repr(SnowflakeConfig(;
        stage="a"
    )) == "SnowflakeConfig(stage=\"a\", opts=ClientOptions())"

    # password is obscured when printing
    @test repr(SnowflakeConfig(;
        stage="a",
        username="b",
        password="c",
        role="d"
    )) == "SnowflakeConfig(stage=\"a\", username=\"b\", password=*****, role=\"d\", opts=ClientOptions())"

    # optional params are supported
    @test repr(SnowflakeConfig(;
        stage="a",
        encryption_scheme="b",
        account="c",
        database="d",
        schema="e",
        endpoint="f"
       )) == "SnowflakeConfig(stage=\"a\", encryption_scheme=\"b\", account=\"c\", database=\"d\", schema=\"e\", endpoint=\"f\", opts=ClientOptions())"
end
