using Quiver

db_file = "test2.sqlite"
time_series_name = "time_series_2"
db = Quiver.create_db(db_file)

n_stages = 100000
n_agents = 500
agent_names = ["Agent_$i" for i in 1:n_agents]
dimensions_names = ["stage"]

dimensions = collect(Int, 1:n_stages)[:, :]
agents = rand(Float64, n_stages, n_agents)

Quiver.create_time_series_table(db, "time_series_2", dimensions_names, agent_names)
@time Quiver.write!(db, "time_series_2", dimensions, agents)

db = nothing
Base.GC.gc()
rm(db_file)