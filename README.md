# RustyObjectStore.jl

[![CI](https://github.com/RelationalAI/RustyObjectStore.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/RelationalAI/RustyObjectStore.jl/actions/workflows/CI.yml)

RustyObjectStore.jl is a Julia package for getting and putting data in cloud object stores, such as Azure Blob Storage and AWS S3.
It is built on top of the Rust [object_store crate](https://docs.rs/object_store/).
It provides a minimal API and focusses on high throughput.

_The package is under active development. Currently only Azure Blob Storage is supported._

## Usage

The object_store runtime must be started before any requests are sent.

```julia
using RustyObjectStore
init_object_store()
```

Requests are sent via calling `put_object` or `get_object!`, providing the location of the object to put/get, either the data to send or a buffer that will receive data, and credentials.
For `put_object` the data must be a vector of bytes (`UInt8`).
For `get_object!` the buffer must be a vector into which bytes (`UInt8`) can be written.
```julia
using RustyObjectStore: get_object!, put_object, AzureConfig

config = AzureConfig(
    storage_account_name="my_account",
    container_name="my_container",
    storage_account_key="my_key"
)
input = "1,2,3,4,5,6,7,8,9,0\n" ^ 5  #  100 B

nbytes_written = put_object(codeunits(input), "path/to/example.csv", config)
@assert nbytes_written == 100

buffer = Vector{UInt8}(undef, 1000)  # 1000 B
@assert sizeof(buffer) > sizeof(input)

nbytes_read = get_object!(buffer, "path/to/example.csv", config)
@assert nbytes_read == 100
@assert String(buffer[1:nbytes_read]) == input
```

One-time global configuration can be set using a StaticConfig object passed to init\_object\_store():
```julia
test_config = StaticConfig(
    n_threads=0,
    cache_capacity=20,
    cache_ttl_secs=30 * 60,
    cache_tti_secs=5 * 60,
    multipart_put_threshold=8 * 1024 * 1024,
    multipart_get_threshold=8 * 1024 * 1024,
    multipart_get_part_size=8 * 1024 * 1024,
    concurrency_limit=512
)
init_object_store(test_config)
```
n\_threads is the number of rust executor threads to use. The default 0 means to use threads equal
to the number of cores.

cache\_capacity is the size of the LRU cache rust uses to cache connection objects. Here a connection
means a unique combination of destination URL, credentials, and per-connection configuration such as
timeouts; it does not mean an HTTP connection.

cache\_ttl\_secs is the time-to-live in seconds for the rust connection cache. Using 0 will disable
ttl eviction.

cache\_tti\_secs is the time in seconds that a connection can be idle before it is removed from the
rust cache. Using 0 will disable tti eviction.

multipart\_put\_threshold is the size in bytes for which any put request over this size will use a
multipart upload. The put part size is determined by the rust object\_store implementation, which
uses 10MB.

multipart\_get\_threshold and multipart\_get\_part\_size configure automatic multipart gets. The part
size can be greater than the threshold without breaking anything, but it may not make sense to do so.
The default 8MB for these values was borrowed from CloudStore.jl.

concurrency\_limit is the max number of concurrent Rust tasks that will be allowed for requests.

## Design

#### Packaging

The Rust [object_store](https://github.com/apache/arrow-rs/tree/master/object_store) crate does not provide a C API, so we have defined a C API in [object_store_ffi](https://github.com/relationalAI/object_store_ffi).
RustyObjectStore.jl depends on [object_store_ffi_jll.jl](https://github.com/JuliaBinaryWrappers/object_store_ffi_jll.jl) to provides a pre-built object_store_ffi library, and calls into the native library via `@ccall`.

#### Rust/Julia Interaction

Julia calls into the native library providing a libuv condition variable and then waits on that variable.
In the native code, the request from Julia is passed into a queue that is processed by a Rust spawned task.
Once the request to cloud storage is complete, Rust signals the condition variable.
In this way, the requests are asynchronous all the way up to Julia and the network processing is handled in the context of native thread pool.

For a GET request, Julia provides a buffer for the native library to write into.
This requires Julia to know a suitable size before-hand and requires the native library to do an extra memory copy, but the upside is that Julia controls the lifetime of the memory.

The library provides a way for Julia code to be notifed about a panic on a Rust thread through the `on_rust_panic` argument of `init_object_store`.
The default behavior is to log the stack trace (if enabled through RUST_BACKTRACE) and exit the process.
The general recommendation is to treat Rust panics as fatal because Julia tasks may hang due to not being notified.

#### Threading Model

Rust object_store uses the [tokio](https://docs.rs/tokio) async runtime.

TODO

#### Rust Configuration

TODO

## Developement

When working on RustyObjectStore.jl you can either use [object_store_ffi_jll.jl](https://github.com/JuliaBinaryWrappers/object_store_ffi_jll.jl) or use a local build of [object_store_ffi](https://github.com/relationalAI/object_store_ffi).
Using object_store_ffi_jll.jl is just like using any other Julia package.
For example, you can change object_store_ffi_jll.jl version by updating the Project.toml `compat` entry and running `Pkg.update` to get the latest compatible release,
or `Pkg.develop` to use an unreleased version.

Alternatively, you can use a local build of object_store_ffi library by setting the `OBJECT_STORE_LIB` environment variable to the location of the build.
For example, if you have the object_store_ffi repository at `~/repos/object_store_ffi` and build the library by running `cargo build --release` from the base of that repository,
then you could use that local build by setting `OBJECT_STORE_LIB="~/repos/object_store_ffi/target/release"`.

The `OBJECT_STORE_LIB` environment variable is intended to be used only for local development.
The library path is set at package precompile time, so if the environment variable is changed RustyObjectStore.jl must recompile for the change to take effect.
You can check the location of the library in use by inspecting `RustyObjectStore.rust_lib`.

Since RustyObjectStore.jl is the primary user of object_store_ffi, the packages should usually be developed alongside one another.
For example, updating object_store_ffi and then testing out the changes in RustyObjectStore.jl.
A new release of object_store_ffi should usually be followed by a new release of object_store_ffi_jll.jl, and then a new release RustyObjectStore.jl.

#### Testing

Tests use the [ReTestItems.jl](https://github.com/JuliaTesting/ReTestItems.jl) test framework.

Run tests using the package manager Pkg.jl like:
```sh
$ julia --project -e 'using Pkg; Pkg.test()'
```
or after starting in a Julia session started with `julia --project`:
```julia
julia> # press ] to enter the Pkg REPL mode

(RustyObjectStore) pkg> test
```
Alternatively, tests can be run using ReTestItems.jl directly, which supports running individual tests.
For example:
```julia
julia> using ReTestItems

julia> runtests("test/azure_api_tests.jl"; name="AzureCredentials")
```

If `OBJECT_STORE_LIB` is set, then running tests locally will use the specified local build of the object_store_ffi library, rather than the version installed by object_store_ffi_jll.jl.
This is useful for testing out changes to object_store_ffi.

Adding new tests is done by writing test code in a `@testitem` in a file suffixed `*_tests.jl`.
See the existing [tests](./test) or the [ReTestItems documentation](https://github.com/JuliaTesting/ReTestItems.jl/#writing-tests) for examples.

#### Release Process

New releases of RustyObjectStore.jl can be made by incrementing the version number in the Project.toml file following [Semantic Versioning](semver.org),
and then commenting on the commit that should be released with `@JuliaRegistrator register`
(see [example](https://github.com/RelationalAI/RustyObjectStore.jl/commit/1b1ba5a198e76afe37f75a1d07e701deb818869c#comments)).
The [JuliaRegistrator](https://github.com/JuliaRegistries/Registrator.jl) bot will reply to the comment and automatically open a PR to the [General](https://github.com/JuliaRegistries/General/) package registry, that should then automatically be merged within a few minutes.
Once that PR to General is merged the new version of RustyObjectStore.jl is available, and the TagBot Github Action will make add a Git tag and a GitHub release for the new version.

RustyObjectStore.jl uses the object_store_ffi library via depending on object_store_ffi_jll.jl which installs pre-built binaries.
So when a new release of object_store_ffi is made, we need there to be a new release of object_store_ffi_jll.jl before we can make a release of RustyObjectStore.jl that uses the latest object_store_ffi.
