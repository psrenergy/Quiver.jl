module TestAqua

using Aqua
using Quiver
using Test

function runtests()
    @testset "Aqua" begin
        @testset "Ambiguities" begin
            Aqua.test_ambiguities(Quiver, recursive = false)
        end
        Aqua.test_all(Quiver, ambiguities = false)
    end
end

TestWriter.runtests()

end
