# You should run the script from the profiling directory

using Profile
using PProf
import Pkg
root_path = dirname(@__DIR__)
Pkg.activate(root_path)
using Quiver
using Test
using Dates

GC.gc()
GC.gc()
rm(joinpath(@__DIR__, "test_read_write.arrow"); force = true)
rm(joinpath(@__DIR__, "test_read_write.csv"); force = true)

function read_write_with_implementation(impl)
    filename = joinpath(@__DIR__, "test_read_write")
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_agents = 3
    
    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        filename,
        dimensions_names,
        agents_names,
        "stage",
        [num_stages, num_scenarios, maximum(num_blocks_per_stage)];
        initial_date = initial_date
    )
    for stage in 1:num_stages
        i = 1
        agents = stage * ones(Float32, num_scenarios * num_blocks_per_stage[stage], num_agents)
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

    reader = QuiverReader{impl}(filename; dimensions_to_cache = [:stage, :scenario]);
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                for ag in 1:num_agents
                    @test reader[ag, block, scenario, stage] == stage
                end
            end
        end
    end
    Quiver.close!(reader)
end

read_write_with_implementation(csv)
read_write_with_implementation(csv)
@profile read_write_with_implementation(csv)
pprof()
