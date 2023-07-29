import Pkg
Pkg.instantiate()

using Revise

Pkg.activate(dirname(@__DIR__))
Pkg.instantiate()

using Quiver
@info("""
This session is using Quiver.jl with Revise.jl.
For more information visit https://timholy.github.io/Revise.jl/stable/.
""")
