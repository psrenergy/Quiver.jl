module TestConvert

using Dates
using Quiver
using Test

function binary_to_csv()
    filename = joinpath(@__DIR__, "test_binary_to_csv")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{Quiver.binary}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer, data; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer)

    Quiver.convert(filename, Quiver.binary, Quiver.csv)

    reader = Quiver.Reader{Quiver.csv}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                Quiver.next_dimension!(reader)
                @test reader.data == [stage, scenario, block]
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename.$(Quiver.file_extension(Quiver.csv))")
    rm("$filename.toml")
end

function csv_to_binary()
    filename = joinpath(@__DIR__, "test_csv_to_binary")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{Quiver.csv}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer, data; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer)

    Quiver.convert(filename, Quiver.csv, Quiver.binary)

    reader = Quiver.Reader{Quiver.binary}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [stage, scenario, block]
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(Quiver.csv))")
    rm("$filename.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename.toml")
end

function test_convert()
    binary_to_csv()
    csv_to_binary()
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

TestConvert.runtests()

end