module TestOperations

using Dates
using Quiver
using Test

function sum_multiple_files(impl)
    filename = joinpath(@__DIR__, "test_sum")
    filenames = [filename * "_$i" for i in 1:3]
    num_files = length(filenames)

    initial_date = DateTime(2024, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1", "agent_2", "agent_1"]

    for file in 1:num_files
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels = [labels[file]],
            time_dimension,
            dimension_size,
            initial_date = initial_date,
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end
        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_summed")
    Quiver.apply_expression(
        output_filename,
        filenames,
        +,
        impl,
    )

    reader = Quiver.Reader{impl}(output_filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [stage + block, scenario]
            end
        end
    end

    Quiver.close!(reader)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end
    rm("$output_filename.$(Quiver.file_extension(impl))")
    rm("$output_filename.toml")

    return nothing
end

function test_operations()
    for impl in Quiver.implementations()
        sum_multiple_files(impl)
    end
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

TestOperations.runtests()

end