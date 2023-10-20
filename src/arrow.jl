# TODO see if we can add compression here
function QuiverWriter{arrow}(
    filename::String,
    dimensions::Vector{String},
    agents::Vector{String};
    initial_date::Union{Nothing, Dates.DateTime} = nothing,
    stage_type::Union{Nothing, String} = nothing,
    unit::Union{Nothing, String} = nothing,
    remove_if_exists::Bool = true
)
    filename_with_extensions = add_extension_to_file(filename, "arrow")
    metadata_with_extensions = add_extension_to_file(filename, "toml")
    rm_if_exists(filename_with_extensions, remove_if_exists)
    rm_if_exists(metadata_with_extensions, remove_if_exists)

    quiver_empty_df = create_quiver_empty_df(dimensions, agents)
    metadata = QuiverMetadata(
        length(dimensions),
        stage_type,
        initial_date,
        unit
    )

    to_toml(metadata_with_extensions, metadata)

    arrow_writer = open(Arrow.Writer, filename_with_extensions)
    Arrow.write(arrow_writer, quiver_empty_df)

    return QuiverWriter{arrow, Arrow.Writer}(
        arrow_writer,
        filename_with_extensions,
        dimensions,
        agents,
        metadata,
        default_last_dimension_added(dimensions)
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
    metadata_with_extension = add_extension_to_file(filename, "toml")
    @assert isfile(filename_with_extension)
    @assert isfile(metadata_with_extension)

    meta_data = from_file(metadata_with_extension)

    tbl = Arrow.Table(filename_with_extension)
    cols = Arrow.names(tbl)

    dimensions = cols[1:meta_data.num_dimensions]
    
    agents_to_read = if agents === nothing
        cols[meta_data.num_dimensions + 1:end]
    else
        agents
    end
    
    df = DataFrames.DataFrame(tbl)

    return QuiverReader{arrow, DataFrame}(
        df,
        dimensions,
        agents_to_read,
        meta_data
    )
end

function _quiver_read(reader::QuiverReader{arrow, DataFrame}, dimensions_to_query::NamedTuple)
    cols_of_agents = find_cols_of_agents(reader, Symbol.(names(reader.reader)))
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
    # The only allocation needed is this last one. But we could also avoid it if we wanted
    # by passing a view. But this would be kind of weird in the context of some applications.
    return Matrix{Float32}(reader.reader[indexes_to_search_at_dimension[end], cols_of_agents])
end