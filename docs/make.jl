import Pkg
Pkg.activate(dirname(@__DIR__))
Pkg.instantiate()
using Quiver

Pkg.activate(@__DIR__)
Pkg.instantiate()
using Documenter

makedocs(;
    modules = [Quiver],
    doctest = true,
    clean = true,
    format = Documenter.HTML(; mathengine = Documenter.MathJax2()),
    sitename = "Quiver.jl",
    warnonly = true,
    pages = [
        "Home" => "index.md",
        "Reading" => "reading.md",
        "Writing" => "writing.md",
        "Examples" => "examples.md",
    ],
)

Documenter.deploydocs(;
    repo = "https://github.com/psrenergy/Quiver.jl.git",  
    push_preview = true,  
)
