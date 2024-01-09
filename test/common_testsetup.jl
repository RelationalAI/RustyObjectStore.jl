@testsetup module InitializeObjectStore
    using RustyObjectStore
    # Since we currently only support centralized configs, we need to have one that is compatible
    # with all the tests (some of the tests would take too long if we use default values).
    max_retries = 2
    retry_timeout_sec = 2
    init_object_store(ObjectStoreConfig(max_retries, retry_timeout_sec))
end
