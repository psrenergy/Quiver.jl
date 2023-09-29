using Quiver
using Dates
using DataFrames
using Arrow
using Random

# just an example of a commonly big file
initial_date = DateTime(2006, 1, 1)
num_stages = 60
dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
num_scenarios = 1200
num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
num_agents = 20
# more or less the size of this file
total_size_gb = 4 * num_stages * num_scenarios * maximum(num_blocks_per_stage) * (num_agents + 3 #= from the dimension columns =#) / 1e9

filename = "test_5gb_variable_block_rand.arrow"
# This should be a vector of symbols
dimensions = ["stage", "scenario", "block"]
agents = ["agent_$i" for i in 1:num_agents]
writer = Quiver.QuiverWriter(filename, dimensions, agents; initial_date = initial_date, stage_type = "month")

# I am allocating everything before so it does not count on the @time

@time for stage in 1:num_stages
    println("$stage")
    i = 1
    agents = rand(Float32, num_scenarios * num_blocks_per_stage[stage], num_agents)
    size_of_batch_write_in_mb = sizeof(agents) / 1e6
    dimensions = Matrix{Int32}(undef, num_scenarios * num_blocks_per_stage[stage], 3)
    for scenario in 1:num_scenarios
        for block in 1:num_blocks_per_stage[stage]
            dimensions[i, 1] = stage
            dimensions[i, 2] = scenario
            dimensions[i, 3] = block
            # This is will create an invalid dimension
            # uncomment this part and it will throw an error
            # if stage == 48
            #     dimensions[i, 1] = 1
            # end
            i += 1
        end
    end
    Quiver.write!(writer, dimensions, agents)
end
Quiver.close!(writer)

@time reader = Quiver.QuiverReader(filename);

# We can query the stage, scenario, block on a random order
# right now it returns a dataframe but it could be a matrix
@time df = Quiver.read(reader, (;stage = 43, scenario = 389, block = 23))
@time df = Quiver.read(reader, (;stage = 22, scenario = 1190, block = 23))

# We can query a stage and scenario and it will return every block from that stage, scenario
@time df = Quiver.read(reader, (;stage = 31, scenario = 234))

# We can query a stage and it will return every scenario and block from that stage
@time df = Quiver.read(reader, (;stage = 21))

# We can query the metadata and other things
Quiver.metadata(filename)
Quiver.schema(filename)
Quiver.names(filename)

### notes
# This should work with negative stages, just haven't tested it yet
# The implementation might not be the best but it is reasonably easy to understand
# It also allows for holes in the data, so if you have a stage 1 and a stage 3 but not a stage 2 it will work. but I haven't tested it yet
# It allows for a variable number of anything, stages, scenarios, blocks, whatever. You can also create your own dimensions as you wish