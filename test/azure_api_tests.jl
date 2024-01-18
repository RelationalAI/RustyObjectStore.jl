@testitem "AzureConfig" begin
    # access key is obscured when printing
    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_account_key="c"
    )) == "AzureConfig(storage_account_name=\"a\", container_name=\"b\", storage_account_key=*****, opts=ClientOptions())"

    # sas token is obscured when printing
    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_sas_token="c"
    )) == "AzureConfig(storage_account_name=\"a\", container_name=\"b\", storage_sas_token=*****, opts=ClientOptions())"

    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_account_key="c",
        host="d"
    )) == "AzureConfig(storage_account_name=\"a\", container_name=\"b\", storage_account_key=*****, host=\"d\", opts=ClientOptions())"

    # can only supply either access key or sas token
    try
        AzureConfig(;
            storage_account_name="a",
            container_name="b",
            storage_account_key="c",
            storage_sas_token="d"
        )
    catch e
        @test e isa ErrorException
        @test e.msg == "Should provide either a storage_account_key or a storage_sas_token"
    end
end
