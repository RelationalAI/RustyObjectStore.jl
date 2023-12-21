# deps/

- `deps/object_store_ffi` -- the Rust library that defines a C API for `object_store`
- `deps/binary_builder` -- the script that uses `BinaryBuilder.jl` to build the `object_store_ffi` binaries and generate the `deps/object_store_ffi_jll.jl` package
- `deps/object_store_ffi_jll.jl` -- the auto-generated Julia package that installs `object_store_ffi` binaries, and on which `ObjectStore.jl` depends.
