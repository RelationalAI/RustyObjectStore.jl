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
