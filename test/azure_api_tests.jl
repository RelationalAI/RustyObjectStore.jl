@testset "AzureCredentials" begin
    # access key is obscured when printing
    @test repr(AzureCredentials("a", "b", "c", "d")) == "AzureCredentials(\"a\", \"b\", \"*****\", \"d\")"
    # host is optional
    @test AzureCredentials("a", "b", "c") == AzureCredentials("a", "b", "c", "")
    # host is not shown if not set
    @test repr(AzureCredentials("a", "b", "c")) == "AzureCredentials(\"a\", \"b\", \"*****\")"
end
