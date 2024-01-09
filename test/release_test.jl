# We should always be using the JLL package.
# We allow seeting the environment variable OBJECT_STORE_LIB to override this
# for development reasons, but it should never be set in CI.
@testitem "Using object_store_ffi_jll" begin
    using object_store_ffi_jll
    @test RustyObjectStore.rust_lib == object_store_ffi_jll.libobject_store_ffi
end
