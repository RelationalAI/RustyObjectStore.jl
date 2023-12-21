# Use baremodule to shave off a few KB from the serialized `.ji` file
baremodule object_store_ffi_jll
using Base
using Base: UUID
import JLLWrappers

JLLWrappers.@generate_main_file_header("object_store_ffi")
JLLWrappers.@generate_main_file("object_store_ffi", UUID("0e112785-0821-598c-8835-9f07837e8d7b"))
end  # module object_store_ffi_jll
