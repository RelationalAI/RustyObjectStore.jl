@testitem "AzureConfig" begin
    # access key is obscured when printing
    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_account_key="c"
    )) == "RustyObjectStore.AzureConfig(Config(:max_retries => \"10\", :url => \"az://b/\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :azure_storage_account_key => \"*****\", :azure_container_name => \"b\", :connect_timeout => \"5s\", :azure_storage_account_name => \"a\"))"

    # sas token is obscured when printing
    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_sas_token="c"
    )) == "RustyObjectStore.AzureConfig(Config(:max_retries => \"10\", :url => \"az://b/\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :azure_storage_sas_token => \"*****\", :azure_container_name => \"b\", :connect_timeout => \"5s\", :azure_storage_account_name => \"a\"))"

    @test repr(AzureConfig(;
        storage_account_name="a",
        container_name="b",
        storage_account_key="c",
        host="d"
    )) == "RustyObjectStore.AzureConfig(Config(:max_retries => \"10\", :url => \"az://b/\", :azurite_host => \"d\", :retry_timeout_secs => \"150\", :timeout => \"30s\", :azure_storage_account_key => \"*****\", :azure_container_name => \"b\", :connect_timeout => \"5s\", :azure_storage_account_name => \"a\"))"

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
