# TODO see if we can add compression here
function QuiverWriter{arrow}(
    filename::String,
    dimension_names::Vector{String},
    agent_names::Vector{String},
    time_dimension::String,
    maximum_value_of_each_dimension::Vector{Int};
    frequency::String = default_frequency(),
    initial_date::Dates.DateTime = default_initial_date(),
    unit::String = default_unit(),
    remove_if_exists::Bool = true
)
    filename_with_extensions = add_extension_to_file(filename, "arrow")
    rm_if_exists(filename_with_extensions, remove_if_exists)

    quiver_empty_df = create_quiver_empty_df(dimension_names, agent_names)

    metadata = QuiverMetadata(;
        # The Quiver API only supports TimeDeltas
        time_representation = TimeDeltas,
        num_dimensions = length(dimension_names),
        frequency,
        initial_date,
        unit,
        time_dimension,
        maximum_value_of_each_dimension
    )
    validate_metadata(metadata)

    metadata_dict = to_dict(metadata)

    arrow_writer = open(Arrow.Writer, filename_with_extensions; metadata = metadata_dict)
    Arrow.write(arrow_writer, quiver_empty_df)

    return QuiverWriter{arrow, Arrow.Writer}(
        arrow_writer,
        filename_with_extensions,
        dimension_names,
        agent_names,
        metadata,
        default_last_dimension_added(dimension_names)
    )
end

function _quiver_write!(writer::QuiverWriter{arrow, Arrow.Writer}, df::DataFrames.DataFrame)
    Arrow.write(writer.writer, df)
    return nothing
end

function _quiver_close!(writer::QuiverWriter{arrow, Arrow.Writer})
    close(writer.writer)
end

function QuiverReader{arrow}(
    filename::String; 
    agents::Union{Nothing, Vector{Symbol}} = nothing,    
)
    filename_with_extension = add_extension_to_file(filename, "arrow")
    @assert isfile(filename_with_extension) "File $filename_with_extension does not exist."

    tbl = Arrow.Table(filename_with_extension)
    metadata = from_dict(Arrow.getmetadata(tbl))
    if metadata.time_representation != TimeDeltas
        error("The Quiver reader API only supports TimeDeltas.")
    end
    validate_metadata(metadata)
    cols = Arrow.names(tbl)
    n_dim = num_dimensions(metadata)

    dimensions = cols[1:n_dim]
    
    agents_to_read = if agents === nothing
        cols[n_dim + 1:end]
    else
        agents
    end
    
    df = DataFrames.DataFrame(tbl)

    return QuiverReader{arrow, DataFrame}(
        df,
        filename,
        dimensions,
        agents_to_read,
        metadata
    )
end

function _quiver_read_df(reader::QuiverReader{arrow, DataFrame}, dimensions_to_query::NamedTuple)
    indexes_to_search_at_dimension = Vector{UnitRange{Int}}(undef, length(dimensions_to_query) + 1)
    indexes_found_at_dimension = Vector{UnitRange{Int}}(undef, length(dimensions_to_query))
    indexes_to_search_at_dimension[1] = 1:size(reader.reader, 1)
    for (i, dim_to_query) in enumerate(dimensions_to_query)
        # The searchsorted function makes a binary search on the indexes_to_search
        # TODO this needs better explanation. It is a series of chained binary searches
        view_of_df = @view reader.reader[indexes_to_search_at_dimension[i], reader.dimensions[i]]
        indexes_found_at_dimension[i] = searchsorted(view_of_df, dim_to_query)
        indexes_to_search_at_dimension[i + 1] = indexes_found_at_dimension[i] .+ indexes_to_search_at_dimension[i][1] .- 1
        if isempty(indexes_found_at_dimension[i])
            error("Dimension $dimensions_to_query not found.")
        end
    end
    return reader.reader[indexes_to_search_at_dimension[end], :]
end

function _quiver_read(reader::QuiverReader{arrow, DataFrame}, dimensions_to_query::NamedTuple)
    cols_of_agents = find_cols_of_agents(reader, Symbol.(names(reader.reader)))
    view_df = _quiver_read_df(reader, dimensions_to_query)
    return Matrix{Float32}(view_df[:, cols_of_agents])
end

function _quiver_close!(reader::QuiverReader{arrow, DataFrame})
    reader.reader = nothing
    return nothing
end