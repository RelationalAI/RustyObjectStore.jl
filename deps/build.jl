using Base.Sys

"""
    assert_compatible_version(cmd, version_specification::String)::Nothing

Accepts a shell command to check a version and a version specification ie: "1.1.1".
Runs the command, seeks the version statement in the second token seperated by a single ' '.

Enforces that this version string is `acceptable` according to the specification via assertion.
Acceptance is based on:
    - Major version must match.
    - Minor and patch versions can be equal or greater then the specified values.
"""
function assert_compatible_version(cmd::Cmd, version_specification::String)::Nothing
    major, minor_min, patch_min = parse.(Int, split(version_specification, "."))
    # Run version command and capture output.
    out_pipe = Pipe()
    err_pipe = Pipe()
    run(pipeline(cmd, stdout=out_pipe, stderr=err_pipe))
    close(out_pipe.in)
    close(err_pipe.in)
    # Read output to string.
    out_str = String(read(out_pipe))
    err_str = String(read(err_pipe))
    # Bubble up any errors that weren't fatal to `run()`.
    (length(err_str) > 0) && Error(err_str)
    # Enforce `acceptable` versions via assertion.
    version_tag = split(out_str, " ")[2]
    major_str, minor_str, patch_str = split(version_tag, ".")
    @assert parse(Int, String(major_str)) == major "$cmd version is not compatible with RAICode."
    @assert parse(Int, String(minor_str)) >= minor_min "$cmd version is not compatible with RAICode."
    if parse(Int, String(minor_str)) == minor_min
        @assert parse(Int, String(patch_str)) >= patch_min "$cmd version is not compatible with RAICode."
    end
end

# Only build Rust code if cargo exists and has a compatible version
print("`which rustup`: "); flush(stdout)
run(pipeline(`which rustup`, stdout=stderr))
run(pipeline(`rustup toolchain install stable --no-self-update`, stdout=stderr))
# run(`which rustup self update`)
run(`rustup default stable`)
print("`which cargo`: "); flush(stdout)

run(pipeline(`which cargo`, stdout=stderr))
assert_compatible_version(`cargo -V`, "1.55.0")
rust_source = joinpath(@__DIR__, "rust_store")
# Elide rust warnings - they aren't helpful in this context
# ENV["RUSTFLAGS"]="-Awarnings"
# build release
cd(rust_source)
if Sys.islinux() || Sys.isapple()
    run(`cargo build --release --verbose`)
end
# rm("target/.rustc_info.json" ; force=true)
