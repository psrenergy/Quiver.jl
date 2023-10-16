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

# This is something for safety but could be turn off
# It helps us guarantee that the dimensions follow their expected behaviour
# There should be smarter ways of doing these checks
function dimensions_are_compliant(db::SQLite.DB, dimensions::Matrix{Int})::Bool
    # The first dimension must be grater or equal than the last one added
    # TODO make a check for the last dimension added
    # first_dim = dimensions[1, :]
    # for (i, dim) in enumerate(first_dim)
    #     if dim > writer.last_dimension_added[i]
    #         # If this is true then everything the order is certainly respected
    #         break
    #     elseif dim == writer.last_dimension_added[i]
    #         # If this is true we still need to check if the next dimension respects the order
    #         continue
    #     else # dim < writer.last_dimension_added[i]
    #        return false
    #     end
    # end

    # The next element of dimensions must be grater or equal than the previous one
    for i in 1:size(dimensions, 1) - 1
        for dim in axes(dimensions, 2)
            if dimensions[i + 1, dim] > dimensions[i, dim]
                break
            elseif dimensions[i + 1, dim] == dimensions[i, dim]
                continue
            else
                return false
            end
        end
    end 

    return true
end

function build_row_id(dimensions::Matrix{Int64})
    rowid = Vector{Int64}(undef, size(dimensions, 1))
    num_dimensions = size(dimensions, 2)
    for i in axes(dimensions, 1)
        id = 0
        for j in axes(dimensions, 2)
            # Each dimension can be encoded in 1e4 different values
            # We could use different values for each dimension
            # but this is a good first approximation
            if !(0 < dimensions[i, j] < 1e4)
                error("Dimensions must be between 1 and 9999.")
            end
            id += dimensions[i, j] * 1e4^(num_dimensions - j)
        end
        rowid[i] = id
    end
    return rowid
end

function build_row_id(dimension::Vector{Int64})
    rowid = 0
    num_dimensions = length(dimension)
    for i in axes(dimension, 1)
        # Each dimension can be encoded in 1e4 different values
        # We could use different values for each dimension
        # but this is a good first approximation
        if !(0 < dimension[i] < 1e4)
            error("Dimensions must be between 1 and 9999.")
        end
        rowid += dimension[i] * 1e4^(num_dimensions - i)
    end
    return rowid
end

function write!(db::SQLite.DB, time_series_name::String, dimensions::Matrix{Int64}, agents::Matrix{Float64})
    if !dimensions_are_compliant(db, dimensions)
        error("Dimensions are not in order.")
    end
    dimensions_names = Symbol.(dimensions_columns(db, time_series_name))
    agents_names = Symbol.(agents_columns(db, time_series_name))
    rowid = build_row_id(dimensions)
    tbl = Tables.table([rowid dimensions agents]; header = [:rowid; dimensions_names; agents_names])
    # TODO
    # The main load of time here is inside the SQLite.bind! part
    # I have the impression that there is a way to avoid it but this is a rabbit hole
    # Also I am sure that jacob quinn et al. have a good reason for doing it this way
    SQLite.load!(tbl, db, time_series_name)
    return nothing
end

# This is an overload in the check names function to allow us to add a rwo_id via the SQLite.load! interface
function SQLite.checknames(
    ::Tables.Schema{names},
    db_names::AbstractVector{String},
) where {names}
    table_names = Set(string.(names))
    push!(db_names, "rowid")
    db_names = Set(db_names)

    if table_names != db_names
        throw(
            SQLiteException(
                "Error loading, column names from table $(collect(table_names)) do not match database names $(collect(db_names))",
            ),
        )
    end
    return true
end

# TODO maybe find a way of making this a kwargs... instead of a NamedTuple
# This is purely for style, instead of searching for Quiver.read(db, (;stage = 231, scenario = 1))
# search for Quiver.read(db; stage = 231, scenario = 1)
function read(db::SQLite.DB, time_series_name::String, dimensions_to_query::NamedTuple)
    dimensions_names = Symbol.(dimensions_columns(db, time_series_name))
    assert_dimensions_are_in_order(db, time_series_name, dimensions_to_query)
    #build first dimension to query
    first_dimension = collect(dimensions_to_query)
    for i in 1:length(dimensions_names)
        if length(first_dimension) < i
            push!(first_dimension, 1)
        end
    end
    first_row_id_in_set = build_row_id(first_dimension)

    # build last dimension to query
    last_dimension = collect(dimensions_to_query)
    for i in 1:length(dimensions_names)
        if length(last_dimension) < i
            push!(last_dimension, 9999)
        end
    end
    last_row_id_in_set = build_row_id(last_dimension)

    # build query
    query = "SELECT * FROM $time_series_name WHERE rowid BETWEEN $first_row_id_in_set AND $last_row_id_in_set"
    query_result = DBInterface.execute(db, query)
    # TODO
    # For some reason this part allocates a lot of memory
    # this is the main bottleneck of the read function
    # return DataFrame(query_result)
end

function assert_dimensions_are_in_order(db::SQLite.DB, time_series_name::String, dimensions_to_query::NamedTuple)
    keys_dims_to_query = keys(dimensions_to_query)
    dimensions_names = Symbol.(dimensions_columns(db, time_series_name))
    for (i, dim) in enumerate(keys_dims_to_query)
        if dim != dimensions_names[i]
            error("Dimensions must be read in the order of the file. (Expected the order $(reader.dimensions)")
        end
    end
    return nothing
end