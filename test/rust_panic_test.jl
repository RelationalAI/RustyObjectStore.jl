@testitem "Handle object_store_ffi panic" begin
    # This needs to be run on a spawned process to ensure proper initialization
    julia_cmd_ffi_panic = Base.julia_cmd()
    code = """
    using Test
    using RustyObjectStore

    triggered = false

    function on_panic()
        global triggered
        triggered = true
    end

    init_object_store(;on_rust_panic=on_panic)

    @test !triggered

    @ccall RustyObjectStore.rust_lib._trigger_panic()::Cint

    @test timedwait(() -> triggered, 0.5) == :ok

    triggered = false

    @ccall RustyObjectStore.rust_lib._trigger_panic()::Cint

    @test timedwait(() -> triggered, 0.5) == :ok
    """
    cmd = `$(julia_cmd_ffi_panic) --startup-file=no --project=. -e $code`
    @test success(pipeline(cmd; stdout=stdout, stderr=stderr))
end
