# TODO add compression here
mutable struct QuiverWriter
    arrow_writer::Arrow.Writer
    dimensions::Vector{Symbol}
    agents::Vector{Symbol}
    initial_date::Union{Nothing, Dates.DateTime}
    stage_type::Union{Nothing, String}
    last_dimension_added::Vector{Int32}
    function QuiverWriter(
        filename::String,
        dimensions::Vector{String},
        agents::Vector{String};
        initial_date::Union{Nothing, Dates.DateTime} = nothing,
        stage_type::Union{Nothing, String} = nothing,
        remove_if_exists::Bool = true
        # TODO 
        # add version and unit
        # add compression, if we turn off is much faster to read (it appears that reading is a bottleneck when compressed)
    )
        # Make a gc pass before starting, not really needed
        # TODO remove
        Base.GC.gc()

        # TODO put inside a function
        if isfile(filename)
            if remove_if_exists
                rm(filename; force = true)
            else
                error("File $filename already exists.")
            end
        end

        # TODO put inside a function
        empty_df_quiver = DataFrames.DataFrame()
        for dim in dimensions
            empty_df_quiver[!, Symbol(dim)] = Int32[]
        end
        for agent in agents
            empty_df_quiver[!, Symbol(agent)] = Float32[]
        end

        # TODO put inside a function
        metadata = Dict{String, String}()
        if stage_type !== nothing
            metadata["stage_type"] = stage_type
        end
        if initial_date !== nothing
            metadata["initial_date"] = string(initial_date)
        end
        metadata["dimension_columns"] = string(length(dimensions))
        metadata["agents_columns"] = string(length(agents))

        arrow_writer = open(Arrow.Writer, filename; metadata = metadata)
        Arrow.write(arrow_writer, empty_df_quiver)

        return new(
            arrow_writer, 
            Symbol.(dimensions), 
            Symbol.(agents), 
            initial_date, 
            stage_type,
            fill(Int32(-10000), length(dimensions))
        )
    end
end

function write!(writer::QuiverWriter, df::DataFrames.DataFrame)
    Arrow.write(writer.arrow_writer, df)
    return nothing
end

function write!(writer::QuiverWriter, dimensions::Matrix{Int32}, agents::Matrix{Float32})
    if !dimensions_are_compliant(writer, dimensions)
        error("Dimensions are not in order.")
    end
    intermediary_df = DataFrames.DataFrame(agents, writer.agents)
    for dim in 1:length(writer.dimensions)
        DataFrames.insertcols!(intermediary_df, dim, writer.dimensions[dim] => dimensions[:, dim])
    end
    Quiver.write!(writer, intermediary_df)
    writer.last_dimension_added = dimensions[end, :]
    return nothing
end

# This is something for safety but could be turn off
# It helps us guarantee that the dimensions follow their expected behaviour
# There should be smarter ways of doing these checks
function dimensions_are_compliant(writer::QuiverWriter, dimensions::Matrix{Int32})::Bool
    # The first dimension must be grater or equal than the last one added
    first_dim = dimensions[1, :]
    for (i, dim) in enumerate(first_dim)
        if dim > writer.last_dimension_added[i]
            # If this is true then everything the order is certainly respected
            break
        elseif dim == writer.last_dimension_added[i]
            # If this is true we still need to check if the next dimension respects the order
            continue
        else # dim < writer.last_dimension_added[i]
           return false
        end
    end

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

function close!(writer::QuiverWriter)
    close(writer.arrow_writer)
    return nothing
end

# Reader
mutable struct QuiverReader
    df::DataFrames.DataFrame
    dimensions::Vector{Symbol}
    agents_to_read::Vector{Symbol}
    metadata::Dict{String, String}

    function QuiverReader(filename::String; 
        agents::Union{Nothing, Vector{Symbol}} = nothing,    
    )
        Quiver.assert_file_exists(filename)
        tbl = Arrow.Table(filename)
        df = DataFrames.DataFrame(tbl)
        metadata = Arrow.getmetadata(tbl)
        cols = Arrow.names(tbl)
        dimensions_cols = parse(Int, metadata["dimension_columns"])
        dimensions = cols[1:dimensions_cols]

        agents_to_read = if agents === nothing
            cols[dimensions_cols + 1:end]
        else
            agents
        end

        return new(
            df,
            dimensions,
            agents_to_read,
            metadata
        )
    end
end

# TODO maybe find a way of making this a kwargs... instead of a NamedTuple
# This is purely for style, instead of searching for Quiver.read(reader, (;stage = 231, scenario = 1))
# search for Quiver.read(reader; stage = 231, scenario = 1)
function read(reader::QuiverReader, dimensions_to_query::NamedTuple)
    assert_dimensions_are_in_order(reader, dimensions_to_query)
    indexes_to_search_at_dimension = Vector{UnitRange{Int}}(undef, length(dimensions_to_query) + 1)
    indexes_found_at_dimension = Vector{UnitRange{Int}}(undef, length(dimensions_to_query))
    indexes_to_search_at_dimension[1] = 1:size(reader.df, 1)
    for (i, dim_to_query) in enumerate(dimensions_to_query)
        # The searchsorted function makes a binary search on the indexes_to_search
        # TODO this needs better explanation. It is a series of chained searches
        # NOTE: The first one is separated from the others because it allocates a lot, 
        # acessing  DataFrame via df[!, dim] is much faster than df[indexes, dim] because it does not allocate
        if i == 1
            indexes_found_at_dimension[i] = searchsorted(reader.df[!, reader.dimensions[i]], dim_to_query)
            indexes_to_search_at_dimension[i + 1] = indexes_found_at_dimension[i]
        else
            indexes_found_at_dimension[i] = searchsorted(reader.df[indexes_to_search_at_dimension[i], reader.dimensions[i]], dim_to_query)
            indexes_to_search_at_dimension[i + 1] = indexes_found_at_dimension[i] .+ indexes_to_search_at_dimension[i][1] .- 1
        end

        if isempty(indexes_found_at_dimension[i])
            error("Dimension $dimensions_to_query not found.")
        end
    end
    return reader.df[indexes_to_search_at_dimension[end], :]
end

function assert_dimensions_are_in_order(reader::QuiverReader, dimensions_to_query::NamedTuple)
    keys_dims_to_query = keys(dimensions_to_query)
    for (i, dim) in enumerate(keys_dims_to_query)
        if dim != reader.dimensions[i]
            error("Dimensions must be read in the order of the file. (Expected the order $(reader.dimensions)")
        end
    end
    return nothing
end

# utils
function names(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Arrow.names
end

function dataframe(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> DataFrames.DataFrame
end

function metadata(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Arrow.getmetadata
end

function schema(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Tables.schema
end

@inline function assert_file_exists(filename::String)
    @assert isfile(filename) "File $filename does not exist."
    return nothing
end