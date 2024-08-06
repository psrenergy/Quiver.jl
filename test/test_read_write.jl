module TestWriter

using Test
using Quiver
using Dates

function read_write_1(impl)
    filename = joinpath(@__DIR__, "test_read_write_1")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3
    
    names_of_dimensions = ["stage", "scenario", "block"]
    names_of_time_series = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    maximum_value_of_each_dimension = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{impl}(
        filename;
        names_of_dimensions,
        names_of_time_series,
        time_dimension,
        maximum_value_of_each_dimension,
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

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                if impl == Quiver.csv
                    Quiver.next_dimension!(reader)
                else
                    Quiver.goto!(reader; stage, scenario, block)
                end
                @test reader.data == [stage, scenario, block]
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_write_2(impl)
    filename = joinpath(@__DIR__, "test_read_write_2")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_segments_per_scenario = [2*s for s in 1:num_scenarios]
    num_time_series = 3
    
    names_of_dimensions = ["stage", "scenario", "block", "segment"]
    names_of_time_series = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    maximum_value_of_each_dimension = [num_stages, num_scenarios, maximum(num_blocks_per_stage), maximum(num_segments_per_scenario)]

    writer = Quiver.Writer{impl}(
        filename;
        names_of_dimensions,
        names_of_time_series,
        time_dimension,
        maximum_value_of_each_dimension,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                for segment in 1:num_segments_per_scenario[scenario]
                    data = [stage, scenario, block + segment]
                    Quiver.write!(writer, data; stage, scenario, block, segment)
                end
            end
        end
    end

    Quiver.close!(writer)

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                for segment in 1:num_segments_per_scenario[scenario]
                    if impl == Quiver.csv
                        Quiver.next_dimension!(reader)
                    else
                        Quiver.goto!(reader; stage, scenario, block, segment)
                    end
                    @test reader.data == [stage, scenario, block + segment]
                end
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_outside_bounds(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_outside_bounds")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    max_num_blocks = maximum(num_blocks_per_stage)
    num_segments_per_scenario = [2*s for s in 1:num_scenarios]
    max_num_segments = maximum(num_segments_per_scenario)
    num_time_series = 3
    
    names_of_dimensions = ["stage", "scenario", "block", "segment"]
    names_of_time_series = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    maximum_value_of_each_dimension = [num_stages, num_scenarios, max_num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
        filename;
        names_of_dimensions,
        names_of_time_series,
        time_dimension,
        maximum_value_of_each_dimension,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                for segment in 1:num_segments_per_scenario[scenario]
                    data = [stage, scenario, block + segment]
                    Quiver.write!(writer, data; stage, scenario, block, segment)
                end
            end
        end
    end
    Quiver.close!(writer)

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:max_num_blocks
                for segment in 1:max_num_segments
                    if block <= num_blocks_per_stage[stage] && segment <= num_segments_per_scenario[scenario]
                        continue
                    end
                    Quiver.goto!(reader; stage, scenario, block, segment)
                    @test all(isnan.(reader.data)) broken=true
                end
            end
        end
    end

    @test_throws EOFError Quiver.goto!(reader; num_stages=num_stages+1, num_scenarios, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios=num_scenarios+1, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, max_num_blocks=max_num_blocks+1, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, max_num_blocks, max_num_segments=max_num_segments+1)

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function test_read_write_implementations()
    for impl in Quiver.implementations()
        read_write_1(impl)
        read_write_2(impl)
        read_outside_bounds(impl)
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

TestWriter.runtests()

end # end module