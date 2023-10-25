function QuiverWriter{csv}(
    filename::String,
    dimensions::Vector{String},
    agents::Vector{String};
    initial_date::Union{Nothing, Dates.DateTime} = nothing,
    stage_type::Union{Nothing, String} = nothing,
    unit::Union{Nothing, String} = nothing,
    remove_if_exists::Bool = true
)
    filename_with_extensions = add_extension_to_file(filename, "csv")
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

    CSV.write(filename_with_extensions, quiver_empty_df)

    return QuiverWriter{csv, Nothing}(
        nothing,
        filename_with_extensions,
        dimensions,
        agents,
        metadata,
        default_last_dimension_added(dimensions)
    )
end

function _quiver_write!(writer::QuiverWriter{csv, Nothing}, df::DataFrames.DataFrame)
    row_iterator = CSV.RowWriter(df)
    open(writer.filename, "a+") do f     
        for (i, row) in enumerate(row_iterator)
            if i == 1
                # avoid writing the header
                continue
            end
            print(f, row)
        end
    end
    return nothing
end

function _quiver_close!(::QuiverWriter{csv, Nothing})
    return nothing
end

function QuiverReader{csv}(
    filename::String; 
    agents::Union{Nothing, Vector{Symbol}} = nothing,    
)
    filename_with_extension = add_extension_to_file(filename, "csv")
    metadata_with_extension = add_extension_to_file(filename, "toml")
    @assert isfile(filename_with_extension)
    @assert isfile(metadata_with_extension)

    meta_data = from_file(metadata_with_extension)

    # The first part is only to get the names
    rows = CSV.Rows(filename_with_extension)
    cols = rows.names

    dimensions = cols[1:meta_data.num_dimensions]
    
    agents_to_read = if agents === nothing
        cols[meta_data.num_dimensions + 1:end]
    else
        agents
    end

    num_agents = length(cols[meta_data.num_dimensions + 1:end])

    _warn_if_file_is_bigger_than_ram(filename_with_extension, "CSV")

    df = CSV.read(filename_with_extension, DataFrame; types = [fill(Int32, meta_data.num_dimensions); fill(Float32, num_agents)])

    return QuiverReader{csv, DataFrame}(
        df,
        dimensions,
        agents_to_read,
        meta_data,
    )
end

function _quiver_read(reader::QuiverReader{csv, DataFrame}, dimensions_to_query::NamedTuple)
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

function _quiver_close!(reader::QuiverReader{csv, DataFrame})
    reader.reader = nothing
    return nothing
end