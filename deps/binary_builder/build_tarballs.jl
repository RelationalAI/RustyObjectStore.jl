using BinaryBuilder

const deps_dir = realpath(joinpath(@__DIR__, ".."))
ENV["JULIA_PKG_DEVDIR"] = deps_dir

name = "object_store_ffi"
version = v"0.1.0"

sources = [
    # DirectorySource(joinpath(deps_dir, "object_store_ffi")),
    # # OR something like this (untested):
    # # https://github.com/RelationalAI/ObjectStore.jl/commit/a5b053f824d8e165211a67c4a84adc520fbac9ae
    GitSource(
        "https://github.com/RelationalAI/ObjectStore.jl/deps/object_store_ffi",
        "b4150213e6b9ce701e05abbd511c92813d90ed62"
    ),
]

# Bash recipe for building across all platforms
script = raw"""
cd ${WORKSPACE}/srcdir/
cargo rustc --release --lib --crate-type=cdylib
install -Dvm 755 "target/${rust_target}/release/libobject_store_ffi.${dlext}" "${libdir}/libobject_store_ffi.${dlext}"
"""

# platforms = supported_platforms()
# # Our Rust toolchain for i686 Windows is unusable
# filter!(p -> !Sys.iswindows(p) || arch(p) != "i686", platforms)
# # Also, can't build cdylib for Musl systems
# filter!(p -> libc(p) != "musl", platforms)

# # For local Mac testing:
platforms = [Platform("aarch64", "macos")]

# The products that we will ensure are always built
products = [
    LibraryProduct("libobject_store_ffi", :libobject_store_ffi),
]

# Dependencies that must be installed before this package can be built
dependencies = [
]

# Build the tarballs
build_tarballs(
    ARGS, name, version, sources, script, platforms, products, dependencies;
    compilers=[:c, :rust], julia_compat="1.6"
)
