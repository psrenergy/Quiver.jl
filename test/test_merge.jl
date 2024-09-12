module TestMerge

using Dates
using Quiver
using Test

function merge_files(impl)
    filename = joinpath(@__DIR__, "test_read_write_merge")
    num_files = 3
    filenames = ["$(filename)_$(i)" for i in 1:num_files]

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    
    for file in 1:num_files
        labels = ["agent_$file"]
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels,
            time_dimension,
            dimension_size,
            initial_date = initial_date
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block + scenario][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end

        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_read_write_merge_merged")
    Quiver.merge(output_filename, filenames, impl)

    reader = Quiver.Reader{impl}(output_filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [stage, scenario, block + scenario]
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
end

function test_merge()
    for impl in Quiver.implementations()
        merge_files(impl)
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

TestMerge.runtests()

end