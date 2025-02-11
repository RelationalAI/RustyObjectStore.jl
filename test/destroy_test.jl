@testitem "destroy_* functions do not panic" setup=[InitializeObjectStore] begin
    result = @ccall RustyObjectStore.rust_lib._destroy_from_julia_thread()::Cint
    @test result == 0
    result = @ccall RustyObjectStore.rust_lib._destroy_in_tokio_thread()::Cint
    @test result == 0
end
