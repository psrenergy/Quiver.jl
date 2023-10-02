function create_db(filepath::String)
    return SQLite.DB(filepath)
end

function create_time_series_table(
    db::SQLite.DB, 
    time_series_name::String, 
    dimensions::Vector{String}, 
    agents::Vector{String}
)
    statement = SQLite.Stmt(db,
        """
        CREATE TABLE IF NOT EXISTS $time_series_name (
            $(join(dimensions, " INTEGER, ")) INTEGER,
            $(join(agents, " REAL, ")) REAL,
            PRIMARY KEY ($(join(dimensions, ", ")))
        )
        """
    )
    DBInterface.execute(statement)
    return time_series_name
end

function dimensions_columns(db::SQLite.DB, time_series_name::String)
    cols = SQLite.columns(db, time_series_name)
    idx = findall(isequal("INTEGER"), cols.type)
    return cols.name[idx]
end

function agents_columns(db::SQLite.DB, time_series_name::String)
    cols = SQLite.columns(db, time_series_name)
    idx = findall(isequal("REAL"), cols.type)
    return cols.name[idx]
end

function write!(db::SQLite.DB, time_series_name::String, dimensions::Matrix{Int64}, agents::Matrix{Float64})
    dimensions_names = Symbol.(dimensions_columns(db, time_series_name))
    agents_names = Symbol.(agents_columns(db, time_series_name))
    tbl = Tables.table([dimensions agents]; header = [dimensions_names; agents_names])
    SQLite.load!(tbl, db, time_series_name)
    return nothing
end