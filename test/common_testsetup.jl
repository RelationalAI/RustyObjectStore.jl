@testsetup module InitializeObjectStore
    using ObjectStore
    # Since we currently only support centralized configs, we need to have one that is compatible
    # with all the tests (some of the tests would take too long if we use default values).
    max_retries = 5
    retry_timeout_sec = 5
    ObjectStore.init_object_store(ObjectStoreConfig(max_retries, retry_timeout_sec))
end
