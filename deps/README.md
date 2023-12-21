# deps/

- `deps/object_store_ffi` -- the Rust library that defines a C API for [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store).
- `deps/binary_builder` -- the script that uses [`BinaryBuilder.jl`](https://github.com/JuliaPackaging/BinaryBuilder.jl) to build the `object_store_ffi` binaries and generate the `object_store_ffi_jll.jl` package.
