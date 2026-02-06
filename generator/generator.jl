import Pkg
Pkg.instantiate()

using Clang.Generators
using Quiver_jll

cd(@__DIR__)

quiver = Quiver_jll.artifact_dir
include_dir = joinpath(quiver, "include", "quiver", "c")

headers = [
    joinpath(include_dir, header) for
    header in readdir(include_dir) if endswith(header, ".h") && header != "platform.h"
]
args = get_default_args()
options = load_options(joinpath(@__DIR__, "generator.toml"))
ctx = create_context(headers, args, options)
build!(ctx)
