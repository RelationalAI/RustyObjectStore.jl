using Clang.Generators
using object_store_ffi_jll: artifact_dir

cd(@__DIR__)

include_dir = normpath(artifact_dir, "include")

# wrapper generator options
options = load_options(joinpath(@__DIR__, "generator.toml"))

# add compiler flags, e.g. "-DXXXXXXXXX"
args = get_default_args()
push!(args, "-I$include_dir")

# Header files to wrap. Assumes object_store_jll has C header file created by cbindgen.
headers = filter(endswith(".h"), readdir(include_dir; join=true))

ctx = create_context(headers, args, options)

# Build without printing so we can rename structs and filter out functions before printing
build!(ctx, BUILDSTAGE_NO_PRINTING)

# Rename structs to have prefix `FFI_` so their usage in Julia code is clear,
# and they can be automatically exported (see generator.toml).
# Remove `ccall` function wrappers as we prefer  `@ccall` usage, see
# https://docs.julialang.org/en/v1/manual/calling-c-and-fortran-code/
filter!(get_nodes(ctx.dag)) do node
    filter!(get_exprs(node)) do expr
        if expr.head == :struct
            expr.args[2] = Symbol(:FFI_, expr.args[2])
        end
        return expr.head != :function
    end
    # Remove nodes with no remaining expressions; otherwise printing fails.
    return !isempty(get_exprs(node))
end

# Print out the file with the renamed structs
build!(ctx, BUILDSTAGE_PRINTING_ONLY)
