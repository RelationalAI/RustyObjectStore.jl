# RustyObjectStore.jl

[![CI](https://github.com/RelationalAI/RustyObjectStore.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/RelationalAI/RustyObjectStore.jl/actions/workflows/CI.yml)

RustyObjectStore.jl is a Julia package for getting and putting data in cloud object stores,
such as Azure Blob Storage and AWS S3.
It is built on top of the Rust [object_store crate](https://docs.rs/object_store/)
It provides a minimal API and focusses on high throughput.

_The package is under active development. Currently only Azure Blob Storage is supported._

## Usage

```julia
input = "1,2,3,4,5,6,7,8,9,0\n" ^ 5  #  100 B
buffer = Vector{UInt8}(undef, 1000)  # 1000 B
@assert sizeof(buffer) > sizeof(input)

credentials = AzureCredentials("my_account", "my_container", "my_key")
nbytes_written = blob_put("path/to/example.csv", codeunits(input), credentials)
@assert nbytes_written == 100

nbytes_read = blob_get!("path/to/example.csv", buffer, credentials)
@assert nbytes_read == 100
@assert String(buffer[1:nbytes_read]) == input
```

## Design

#### Threading Model

Rust object_store uses the [tokio](https://docs.rs/tokio) async runtime.

TODO

#### Rust/Julia Interaction

Julia calls into the native library providing a libuv condition variable and then waits on that variable.
In the native code, the request from Julia is passed into a queue that is processed by a Rust spawned task.
Once the request to cloud storage is complete, Rust signals the condition variable.
In this way, the requests are asynchronous all the way up to Julia and the network processing is handled in the context of native thread pool.

For a GET request, Julia provides a buffer for the native library to write into.
This requires Julia to know a suitable size before-hand and requires the native library to do an extra memory copy, but the upside is that Julia controls the lifetime of the memory.

#### Rust Configuration

TODO

#### Packaging

The Rust object_store crate does not provide a C API, so we have defined a C API in [object_store_ffi](https://github.com/relationalAI/object_store_ffi).
RustyObjectStore.jl depends on [object_store_ffi_jll.jl](https://github.com/JuliaBinaryWrappers/object_store_ffi_jll.jl)
to provides a pre-built object_store_ffi library.
RustyObjectStore.jl calls into the native library via `@ccall`.

## Developement

TODO

#### Testing

TODO

#### Release Process

TODO
