using BinaryBuilder

name = "rust_store"
version = v"0.1.0"

sources = [
    DirectorySource("./rust_store"),
    # # OR something like this (untested):
    # # https://github.com/RelationalAI/ObjectStore.jl2/commit/a5b053f824d8e165211a67c4a84adc520fbac9ae
    # GitSource(
    #     "https://github.com/RelationalAI/ObjectStore.jl2/deps/rust_store",
    #     "a5b053f824d8e165211a67c4a84adc520fbac9ae"
    # ),
]

# Bash recipe for building across all platforms
script = raw"""
cd ${WORKSPACE}/srcdir/
cargo rustc --release --lib --crate-type=cdylib
install -Dvm 755 "target/${rust_target}/release/librust_store.${dlext}" "${libdir}/librust_store.${dlext}"
"""

platforms = supported_platforms()
# Our Rust toolchain for i686 Windows is unusable
filter!(p -> !Sys.iswindows(p) || arch(p) != "i686", platforms)
# Also, can't build cdylib for Musl systems
filter!(p -> libc(p) != "musl", platforms)
# # For local Mac testing:
# platforms = [Platform("aarch64", "macos")]

# The products that we will ensure are always built
products = [
    LibraryProduct("librust_store", :librust_store),
]

# Dependencies that must be installed before this package can be built
dependencies = Dependency[
]

# Build the tarballs
build_tarballs(ARGS, name, version, sources, script, platforms, products, dependencies; compilers=[:c, :rust], julia_compat="1.6")
