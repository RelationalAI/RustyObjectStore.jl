using BinaryBuilder

cd(@__DIR__)

const deps_dir = ".."

# So `--deploy=local` puts the JLL package in the `deps/` directory.
ENV["JULIA_PKG_DEVDIR"] = deps_dir

name = "object_store_ffi"
version = v"0.1.0"

sources = [
    DirectorySource(joinpath(deps_dir, "object_store_ffi")),
]

# Bash recipe for building across all platforms
script = raw"""
cd ${WORKSPACE}/srcdir/
cargo rustc --release --lib --crate-type=cdylib
install -Dvm 755 "target/${rust_target}/release/libobject_store_ffi.${dlext}" "${libdir}/libobject_store_ffi.${dlext}"
"""

# We could potentially support more platforms, if required.
# Except perhaps i686 Windows and Musl systems.
platforms = [
    Platform("aarch64", "macos"),
    Platform("x86_64",  "linux"),
    # Platform("x86_64",  "macos"),
    # Platform("aarch64", "linux"),
]

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
