import Pkg
Pkg.activate(dirname(@__DIR__))
Pkg.instantiate()
using Quiver

Pkg.activate(@__DIR__)
Pkg.instantiate()
using Documenter

makedocs(;
    modules = [GNoMo],
    doctest = false,
    clean = true,
    format = Documenter.HTML(;
        mathengine = Documenter.MathJax2(),
        prettyurls = false,
        # Prevents the edit on github button from showing up
        # edit_link = nothing,
        # footer = nothing,
        # disable_git = true,
        # repolink = nothing,
    ),
    sitename = "GNoMo.jl",
    warnonly = true,
    pages = [
        "Home" => [
            "Overview" => "home.md",
        ],
        "Manual" => [
            "Reading Data" => "reading.md",
            "Writing Data" => "writing.md",
            "Examples" => "examples.md",
        ],
    ],
)

Documenter.deploydocs(;
    repo = "https://github.com/psrenergy/Quiver.jl.git", 
    branch = "gh-pages", 
    push_preview = true, 
)