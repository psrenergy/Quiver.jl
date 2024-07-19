module SQLiteDoneRight

using SQLite

mutable struct QuiverSQLiteWriter
    db::SQLite.DB
    time_series_name::String
    num_stages::Int
    num_scenarios::Int
    num_blocks::Int
    agents_names::Vector{String}
    prepared_statement::SQLite.Stmt
    data_to_add::Vector{Float64}
    stmt_handle
end

mutable struct QuiverSQLiteReader
    db::SQLite.DB
    data::Vector{Float64}
end

function encode_dimensions(stage::Integer, scenario::Integer, block::Integer)
    return stage + (scenario << 13) + (block << (13 + 13))
end

function decode_dimensions(encoded::Integer)
    return [encoded & 0x1FFF, (encoded >> 13) & 0x1FFF, (encoded >> (13 + 13)) & 0x1FFF]
end

function crate_sqlite_time_series_writer(
    FILE_PATH,
    num_stages::Int,
    num_scenarios::Int,
    num_blocks::Int,
    agents_names::Vector{String},
)
    db = SQLite.DB("./$FILE_PATH.sqlite")
    DBInterface.execute(db, "PRAGMA synchronous = OFF")
    statement = SQLite.Stmt(db,
        """
        CREATE TABLE IF NOT EXISTS time_series (
            encoded INTEGER PRIMARY KEY,
            stage INTEGER,
            scenario INTEGER,
            block INTEGER,
            $(join(agents_names, " REAL, ")) REAL
        )
        """
    )
    DBInterface.execute(statement)

    stmt =  """
    INSERT INTO time_series (encoded, stage, scenario, block, $(join(agents_names, ", ")))
    VALUES (?, ?, ?, ?, $(join(["?" for _ in 1:length(agents_names)], ", ")))
    """

    prepared_statement = SQLite.Stmt(db, stmt)

    return QuiverSQLiteWriter(db, "time_series", num_stages, num_scenarios, num_blocks, agents_names, prepared_statement, zeros(Float64, length(agents_names) + 4), SQLite._get_stmt_handle(prepared_statement))
end

function write_registry(iow::QuiverSQLiteWriter, values::Vector{Float64}, stage::Integer, scenario::Integer, block::Integer)
    encoded = encode_dimensions(stage, scenario, block)
    SQLite.C.sqlite3_bind_int64(iow.stmt_handle, 1, encoded)
    SQLite.C.sqlite3_bind_int64(iow.stmt_handle, 2, stage)
    SQLite.C.sqlite3_bind_int64(iow.stmt_handle, 3, scenario)
    SQLite.C.sqlite3_bind_int64(iow.stmt_handle, 4, block)
    for i in 1:length(iow.agents_names)
        SQLite.C.sqlite3_bind_double(iow.stmt_handle, 4 + i, values[i])
    end
    r = SQLite.C.sqlite3_step(iow.stmt_handle)
    if r == SQLite.C.SQLITE_DONE
        SQLite.C.sqlite3_reset(iow.stmt_handle)
    elseif r != SQLite.C.SQLITE_ROW
        e = SQLite.sqliteexception(iow.db, iow.prepared_statement)
        SQLite.C.sqlite3_reset(iow.stmt_handle)
        throw(e)
    end
    return nothing
end

function create_sqlite_time_series_reader(FILE_PATH)
    db = SQLite.DB("./$FILE_PATH.sqlite")
    return QuiverSQLiteReader(db, [])
end 

function goto(ior::QuiverSQLiteReader, stage::Integer, scenario::Integer, block::Integer)
    rowid = encode_dimensions(stage, scenario, block)
    statement = SQLite.Stmt(ior.db,
        """
        SELECT * FROM time_series WHERE rowid = $rowid
        """
    )
    ior.data = DBInterface.execute(statement)
end

end