using BinaryBuilder

name = "object_store"
version = v"0.8.0"

# checksum: curl -L <path> | shasum -a 256
# curl -L https://dist.apache.org/repos/dist/release/arrow/arrow-object-store-rs-0.8.0/apache-arrow-object-store-rs-0.8.0.tar.gz | shasum -a 256
sources = [
    ArchiveSource(
        "https://dist.apache.org/repos/dist/release/arrow/arrow-object-store-rs-$(version)/apache-arrow-object-store-rs-$(version).tar.gz",
        "d3bda552b01ac54e2915e2c51d0158f7c16a11994753f5dff2000ae859e82dce"
    ),
    # # OR
    # # https://github.com/apache/arrow-rs/commit/ad211fe324d259bf9fea1c43a3a82b3c833f6d7a
    # GitSource(
    #     "https://github.com/apache/arrow-rs/tree/master/object_store",
    #     "ad211fe324d259bf9fea1c43a3a82b3c833f6d7a"
    # ),
]

# Bash recipe for building across all platforms
# cargo rustc --release --lib --crate-type=lib,staticlib,cdylib
script = raw"""
cd ${WORKSPACE}/srcdir/apache-arrow-object-store-rs-*/
cargo rustc --release --lib --crate-type=cdylib
install -Dvm 755 "target/${rust_target}/release/libobject_store.${dlext}" "${libdir}/libobject_store.${dlext}"
"""

platforms = supported_platforms()
# Our Rust toolchain for i686 Windows is unusable
filter!(p -> !Sys.iswindows(p) || arch(p) != "i686", platforms)
# Also, can't build cdylib for Musl systems
filter!(p -> libc(p) != "musl", platforms)
# # For local mac testing:
# platforms = [Platform("aarch64", "macos")]

# The products that we will ensure are always built
products = [
    LibraryProduct("libobject_store", :libobject_store),
]

# Dependencies that must be installed before this package can be built
dependencies = Dependency[
]

# Build the tarballs
build_tarballs(ARGS, name, version, sources, script, platforms, products, dependencies; compilers=[:c, :rust], julia_compat="1.6")
