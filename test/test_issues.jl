module TestIssues

using Dates
using Random
using Quiver
using Test

function test_issue_40()
    filename = joinpath(@__DIR__, "test_issue_40")
    stages = 4

    for impl in Quiver.implementations()
        writer = Quiver.Writer{impl}(
            filename;
            dimensions = ["stage"],
            dimension_size = [stages],
            labels = ["agent 1"],
            time_dimension = "stage",
        )

        for stage in 1:stages
            @test_throws ArgumentError Quiver.write!(writer, [stage, stage]; stage = stage)
        end
        Quiver.close!(writer)

        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end

    return nothing
end

function runtests()
    Base.GC.gc()
    Base.GC.gc()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

TestIssues.runtests()

end