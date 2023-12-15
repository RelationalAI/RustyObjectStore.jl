using Clang.Generators
using rust_store_jll

cd(@__DIR__)

include_dir = normpath(rust_store_jll.artifact_dir, "include")

# wrapper generator options
options = load_options(joinpath(@__DIR__, "generator.toml"))

# add compiler flags, e.g. "-DXXXXXXXXX"
args = get_default_args()
push!(args, "-I$include_dir")

# header files to wrap
headers = joinpath(include_dir, "rust_store.h")

ctx = create_context(headers, args, options)

build!(ctx)

# # build without printing so we can rename struct before printing
# build!(ctx, BUILDSTAGE_NO_PRINTING)

# function rewrite!(dag::ExprDAG)
#     for node in get_nodes(dag)
#         for expr in get_exprs(node)
#             rewrite!(expr)
#         end
#     end
# end
# function rewrite!(ex::Expr)
#     if ex.head == :struct
#         ex.args[2] = Symbol(ex.args[2], :_FFI)
#     end
#     # TODO: use these renamed structs in the `ccall`s
#     return ex
# end

# rewrite!(ctx.dag)

# # print out the file with the renamed structs
# build!(ctx, BUILDSTAGE_PRINTING_ONLY)
