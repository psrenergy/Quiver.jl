module TestIssues

using Dates
using Random
using Quiver
using Test

function test_issue_43()
    filename = joinpath(@__DIR__, "test_issue_43")

    stages = 4
    scenarios = 3
    blocks = 5

    Random.seed!(1)
    random_stages = shuffle([stage for stage in 1:stages])
    random_scenarios = shuffle([scenario for scenario in 1:scenarios])
    random_blocks = shuffle([block for block in 1:blocks])

    writer = Quiver.Writer{Quiver.binary}(
        filename;
        dimensions = ["stage", "scenario", "block"],
        dimension_size = [stages, scenarios, blocks],
        labels = ["stage", "scenario", "block"],
        time_dimension = "stage",
    )

    for stage in random_stages
        for scenario in random_scenarios
            for block in random_blocks
                Quiver.write!(writer, [stage, scenario, block]; stage = stage, scenario = scenario, block = block)
            end
        end
    end
    Quiver.close!(writer)

    reader = Quiver.Reader{Quiver.binary}(filename)
    for stage in 1:stages
        for scenario in 1:scenarios
            for block in 1:blocks
                data = Quiver.goto!(reader; stage = stage, scenario = scenario, block = block)
                @test data == [stage, scenario, block]
            end
        end
    end
    Quiver.close!(reader)

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
