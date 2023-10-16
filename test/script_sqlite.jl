using Quiver

db_file = "test3.sqlite"
time_series_name = "time_series_2"
db = Quiver.create_db(db_file)

n_stages = 1000
n_agents = 50
agent_names = ["Agent_$i" for i in 1:n_agents]
dimensions_names = ["stage", "scenario"]

dimensions = [collect(Int, 1:n_stages) ones(Int, n_stages)]
agents = rand(Float64, n_stages, n_agents)

Quiver.create_time_series_table(db, "time_series_2", dimensions_names, agent_names)
@time Quiver.write!(db, "time_series_2", dimensions, agents)

# Read stage 400
@time Quiver.read(db, "time_series_2", (;stage = 400));

db = nothing
Base.GC.gc()
rm(db_file)

# just an example of a commonly big file
using Dates
initial_date = DateTime(2006, 1, 1)
num_stages = 60
dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
num_scenarios = 1200
num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
num_agents = 20
# more or less the size of this file
total_size_gb = 4 * num_stages * num_scenarios * maximum(num_blocks_per_stage) * (num_agents + 3 #= from the dimension columns =#) / 1e9

filename = "test_varies_by_block.sqlite"
# This should be a vector of symbols
dimensions_names = ["stage", "scenario", "block"]
agents_names = ["agent_$i" for i in 1:num_agents]
db = Quiver.create_db(filename)
time_series_name = "time_series"
Quiver.create_time_series_table(db, time_series_name, dimensions_names, agents_names)

# I am allocating everything before so it does not count on the @time

@time for stage in 1:num_stages
    println("$stage")
    i = 1
    local agents = rand(Float64, num_scenarios * num_blocks_per_stage[stage], num_agents)
    size_of_batch_write_in_mb = sizeof(agents) / 1e6
    local dimensions = Matrix{Int64}(undef, num_scenarios * num_blocks_per_stage[stage], 3)
    for scenario in 1:num_scenarios
        for block in 1:num_blocks_per_stage[stage]
            dimensions[i, 1] = stage
            dimensions[i, 2] = scenario
            dimensions[i, 3] = block
            i += 1
        end
    end
    Quiver.write!(db, time_series_name, dimensions, agents)
end

# We can query the stage, scenario, block on a random order
# right now it returns a dataframe but it could be a matrix
@time df = Quiver.read(db, time_series_name, (;stage = 43, scenario = 39, block = 23))
@time df = Quiver.read(db, time_series_name, (;stage = 22, scenario = 28, block = 23))

# We can query a stage and scenario and it will return every block from that stage, scenario
@time df = Quiver.read(db, time_series_name, (;stage = 31, scenario = 1))

# We can query a stage and it will return every scenario and block from that stage
@time df = Quiver.read(db, time_series_name, (;stage = 21))

@time begin 
    for stage in 1:num_stages
        println("$stage")
        for scenario in 1:num_scenarios
            Quiver.read(db, time_series_name, (;stage = stage, scenario = scenario))
        end
    end
end


# test with PSRI
using PSRClassesInterface
PSRI = PSRClassesInterface
FILE_PATH = "./test_5gb_variable_block_rand"
iow = PSRI.open(
    PSRI.OpenBinary.Writer,
    FILE_PATH,
    is_hourly = true,
    scenarios = num_scenarios,
    stages = num_stages,
    agents = agents_names,
    unit = "",
    initial_stage = 1,
    initial_year = 2006,
)

@time for stage in 1:num_stages
    println("$stage")
    agents = rand(Float32, num_scenarios * PSRI.blocks_in_stage(iow, stage), num_agents)
    for scenario in 1:num_scenarios
        for block in 1:PSRI.blocks_in_stage(iow, stage)
            PSRI.write_registry(
                iow,
                # This is wrong but is ok for performance testing
                agents[scenario, :],
                stage,
                scenario,
                block,
            )
        end
    end
end

PSRI.close(iow)

ior = PSRI.open(
    PSRI.OpenBinary.Reader, 
    FILE_PATH;
    use_header = false
)

@time for stage = 1:num_stages
    println("$stage")
    for scenario = 1:num_scenarios, block = 1:PSRI.blocks_in_stage(iow, stage)
        ior.data
        PSRI.next_registry(ior)
    end
end

PSRI.close(ior)