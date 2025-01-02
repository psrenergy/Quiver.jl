using Aqua
using Quiver
using Test

function test_modules(dir::AbstractString)
    result = String[]
    for (root, dirs, files) in walkdir(dir)
        append!(result, filter!(f -> occursin(r"test_(.)+\.jl", f), joinpath.(root, files)))
    end
    return result
end

@testset "Aqua" begin
    @testset "Ambiguities" begin
        Aqua.test_ambiguities(Quiver, recursive = false)
    end
    Aqua.test_all(Quiver, ambiguities = false)
end

for file in test_modules(@__DIR__)
    @testset "$(basename(file))" begin
        include(file)
    end
end
