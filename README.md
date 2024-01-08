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
