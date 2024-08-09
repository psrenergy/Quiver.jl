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
    
    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{impl}(
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
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage), maximum(num_segments_per_scenario)]

    writer = Quiver.Writer{impl}(
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

function read_write_3(impl)
    filename = joinpath(@__DIR__, "test_read_write_3")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    max_num_blocks = maximum(num_blocks_per_stage)
    num_segments_per_block = [round(Int, b/20) for b in 1:max_num_blocks]
    max_num_segments = maximum(num_segments_per_block)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, max_num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
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
                for segment in 1:num_segments_per_block[block]
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
                for segment in 1:num_segments_per_block[block]
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

function read_write_4(impl)
    filename = joinpath(@__DIR__, "test_read_write_4")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios_per_stage = [2*s + 10 for s in 1:num_stages]
    max_num_scenarios = maximum(num_scenarios_per_stage)
    num_blocks = 24
    num_segments_per_block = [round(Int, b/20) for b in 1:num_blocks]
    max_num_segments = maximum(num_segments_per_block)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, max_num_scenarios, num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios_per_stage[stage]
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block[block]
                    data = [stage, scenario, block + segment]
                    Quiver.write!(writer, data; stage, scenario, block, segment)
                end
            end
        end
    end

    Quiver.close!(writer)

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios_per_stage[stage]
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block[block]
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

function read_write_5(impl)
    filename = joinpath(@__DIR__, "test_read_write_5")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_segments_per_block_scenario = [s + b for b in 1:num_blocks, s in 1:num_scenarios]
    max_num_segments = maximum(num_segments_per_block_scenario)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block_scenario[block, scenario]
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
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block_scenario[block, scenario]
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

function read_write_carrousel(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_write_1")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 1
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
        # carrousel = true
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

    number_of_stages_to_read = 50
    number_of_scenarios_to_read = 10

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:number_of_stages_to_read
        inbounds_stage = mod1(stage, num_stages)
        for scenario in 1:number_of_scenarios_to_read
            inbounds_scenario = mod1(scenario, num_scenarios)
            for block in 1:num_blocks_per_stage[inbounds_stage]
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [inbounds_stage, inbounds_scenario, block] #skip=(stage > num_stages || scenario > num_scenarios)
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_outside_bounds_1(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_outside_bounds_1")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    max_num_blocks = maximum(num_blocks_per_stage)
    num_segments_per_scenario = [2*s for s in 1:num_scenarios]
    max_num_segments = maximum(num_segments_per_scenario)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, max_num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
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

    @test_throws EOFError Quiver.goto!(reader; stage = num_stages+1, num_scenarios, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, scenario = num_scenarios+1, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, block = max_num_blocks+1, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, max_num_blocks, segment = max_num_segments+1)

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_outside_bounds_2(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_outside_bounds_2")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    max_num_blocks = maximum(num_blocks_per_stage)
    num_segments_per_block = [round(Int, b/20) for b in 1:max_num_blocks]
    max_num_segments = maximum(num_segments_per_block)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, max_num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
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
                for segment in 1:num_segments_per_block[block]
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
                    if block <= num_blocks_per_stage[stage] && segment <= num_segments_per_block[block]
                        continue
                    end
                    Quiver.goto!(reader; stage, scenario, block, segment)
                    @test all(isnan.(reader.data)) broken=true
                end
            end
        end
    end

    @test_throws EOFError Quiver.goto!(reader; stage = num_stages+1, num_scenarios, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, scenario = num_scenarios+1, max_num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, block = max_num_blocks+1, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, max_num_blocks, segment = max_num_segments+1)

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_outside_bounds_3(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_outside_bounds_3")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios_per_stage = [2*s + 10 for s in 1:num_stages]
    max_num_scenarios = maximum(num_scenarios_per_stage)
    num_blocks = 24
    num_segments_per_block = [round(Int, b/20) for b in 1:num_blocks]
    max_num_segments = maximum(num_segments_per_block)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, max_num_scenarios, num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios_per_stage[stage]
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block[block]
                    data = [stage, scenario, block + segment]
                    Quiver.write!(writer, data; stage, scenario, block, segment)
                end
            end
        end
    end

    Quiver.close!(writer)

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:max_num_scenarios
            for block in 1:num_blocks
                for segment in 1:max_num_segments
                    if scenario <= num_scenarios_per_stage[stage] && segment <= num_segments_per_block[block]
                        continue
                    end
                    Quiver.goto!(reader; stage, scenario, block, segment)
                    @test all(isnan.(reader.data)) broken=true
                end
            end
        end
    end

    @test_throws EOFError Quiver.goto!(reader; stage = num_stages+1, max_num_scenarios, num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, scenario = max_num_scenarios+1, num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, max_num_scenarios, block = num_blocks+1, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, max_num_scenarios, num_blocks, segment = max_num_segments+1)

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_outside_bounds_4(impl)
    if impl == Quiver.csv
        return
    end
    filename = joinpath(@__DIR__, "test_read_outside_bounds_4")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_segments_per_block_scenario = [s + b for b in 1:num_blocks, s in 1:num_scenarios]
    max_num_segments = maximum(num_segments_per_block_scenario)
    num_time_series = 3
    
    dimensions = ["stage", "scenario", "block", "segment"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks, max_num_segments]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                for segment in 1:num_segments_per_block_scenario[block, scenario]
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
            for block in 1:num_blocks
                for segment in 1:max_num_segments
                    if segment <= num_segments_per_block_scenario[block, scenario]
                        continue
                    end
                    Quiver.goto!(reader; stage, scenario, block, segment)
                    @test all(isnan.(reader.data)) broken=true
                end
            end
        end
    end

    @test_throws EOFError Quiver.goto!(reader; stage = num_stages+1, num_scenarios, num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, scenario = num_scenarios+1, num_blocks, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, block = num_blocks+1, max_num_segments)
    @test_throws EOFError Quiver.goto!(reader; num_stages, num_scenarios, num_blocks, segment = max_num_segments+1)

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function read_filtering_labels(impl)
    filename = joinpath(@__DIR__, "test_read_filtering_labels")

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

    writer = Quiver.Writer{impl}(
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

    reader = Quiver.Reader{impl}(filename; labels_to_read = ["agent_1", "agent_3"])
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                if impl == Quiver.csv
                    Quiver.next_dimension!(reader)
                else
                    Quiver.goto!(reader; stage, scenario, block)
                end
                @test reader.data == [stage, block]
            end
        end
    end

    Quiver.close!(reader)

    reader = Quiver.Reader{impl}(filename; labels_to_read = ["agent_2", "agent_1"])
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                if impl == Quiver.csv
                    Quiver.next_dimension!(reader)
                else
                    Quiver.goto!(reader; stage, scenario, block)
                end
                @test reader.data == [scenario, stage]
            end
        end
    end

    Quiver.close!(reader)

    reader = Quiver.Reader{impl}(filename; labels_to_read = ["agent_2", "agent_1", "agent_3"])
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                if impl == Quiver.csv
                    Quiver.next_dimension!(reader)
                else
                    Quiver.goto!(reader; stage, scenario, block)
                end
                @test reader.data == [scenario, stage, block]
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
end

function test_read_write_implementations()
    for impl in Quiver.implementations()
        @testset "Read and Write $(impl)" begin
            read_write_1(impl)
            read_write_2(impl)
            read_write_3(impl)
            read_write_4(impl)
            read_write_5(impl)
            # read_write_carrousel(impl)
            read_outside_bounds_1(impl)
            read_outside_bounds_2(impl)
            read_outside_bounds_3(impl)
            read_outside_bounds_4(impl)
            read_filtering_labels(impl)
        end
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