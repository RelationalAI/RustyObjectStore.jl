using Clang.Generators
using object_store_ffi_jll: artifact_dir

cd(@__DIR__)

include_dir = normpath(artifact_dir, "include")

# wrapper generator options
options = load_options(joinpath(@__DIR__, "generator.toml"))

# add compiler flags, e.g. "-DXXXXXXXXX"
args = get_default_args()
push!(args, "-I$include_dir")

# Header files to wrap. Assumes object_store_ffi_jll has C header file created by cbindgen.
headers = filter(endswith(".h"), readdir(include_dir; join=true))

ctx = create_context(headers, args, options)

build!(ctx)
