function sqlite_write_2d_one_shot(file_name::String, arr::Matrix{Float32}, agent_names::Vector{String})
    n_agents = length(agent_names)
    
    stages = collect(Int64, 1:size(arr, 1))
    df = DataFrame(stages[:, :], ["stage"])
    for i in 1:n_agents
        df[!, agent_names[i]] = arr[:, i]
    end
    
    rm(file_name*".sqlite3", force = true)
    db = SQLite.DB(file_name*".sqlite3")
    SQLite.load!(df, db, file_name)
    DBInterface.close!(db)

    return nothing
end