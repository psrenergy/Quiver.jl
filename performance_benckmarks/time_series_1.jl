using Quiver
using Dates

const FILENAME = "time_series_1"

function write_time_series(impl::Type{<:Quiver.QuiverImplementation})
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 1200
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_agents = 20
    
    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        FILENAME,
        dimensions_names,
        agents_names,
        ["stage", "block"],
        [num_stages, num_scenarios, maximum(num_blocks_per_stage)],
    )
    for stage in 1:num_stages
        i = 1
        agents = rand(Float32, num_scenarios * num_blocks_per_stage[stage], num_agents)
        dimensions = Matrix{Int32}(undef, num_scenarios * num_blocks_per_stage[stage], 3)
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                dimensions[i, 1] = stage
                dimensions[i, 2] = scenario
                dimensions[i, 3] = block
                i += 1
            end
        end
        Quiver.write!(writer, dimensions, agents)
    end
    Quiver.close!(writer)
end

function read_time_series(impl::Type{<:Quiver.QuiverImplementation})
    reader = QuiverReader{impl}(FILENAME);
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            Quiver.read(reader, (;stage = stage, scenario = scenario))
        end
    end
    Quiver.close!(reader)
end

for impl in Quiver.implementations()
    println(impl)
    println("write time")
    @time write_time_series(impl);
    println("read time")
    @time read_time_series(impl);
    Base.GC.gc()
end