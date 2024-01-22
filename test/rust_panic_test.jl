@testitem "Handle object_store_ffi panic" begin
    using RustyObjectStore

    triggered = false

    function on_panic()
        global triggered
        triggered = true
    end

    init_object_store(;on_rust_panic=on_panic)

    @test !triggered

    @ccall RustyObjectStore.rust_lib._trigger_panic()::Cint

    sleep(0.5)

    @test triggered
end
