function QuiverWriter{csv}(
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
    filename_with_extensions = add_extension_to_file(filename, "csv")
    rm_if_exists(filename_with_extensions, remove_if_exists)

    metadata = QuiverMetadata(;
        num_dimensions = length(dimension_names),
        frequency,
        initial_date,
        unit,
        time_dimension,
        maximum_value_of_each_dimension
    )

    quiver_empty_df = _create_quiver_empty_df(dimension_names, agent_names)
    # Save header on buffer
    iobuf = IOBuffer()
    CSV.write(iobuf, quiver_empty_df)

    # Write metadata on the top of the file
    open(filename_with_extensions, "a+") do io
        print(io, to_string(metadata))
        print(io, String(take!(iobuf)))
    end

    return QuiverWriter{csv, Nothing}(
        nothing,
        filename_with_extensions,
        dimension_names,
        agent_names,
        metadata,
        default_last_dimension_added(dimension_names)
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
    dimensions_to_cache::Union{Nothing, Vector{Symbol}} = nothing
)
    filename_with_extension = add_extension_to_file(filename, "csv")
    @assert isfile(filename_with_extension) "File $filename_with_extension does not exist."

    metadata_string = readuntil(open(filename_with_extension), "--- \n")
    metadata = from_string(metadata_string)
    header_line = 9

    # The first part is only to get the names
    rows = CSV.Rows(filename_with_extension; header = header_line)
    cols = rows.names
    n_dim = num_dimensions(metadata)

    dimensions = cols[1:n_dim]
    
    agents_to_read = if agents === nothing
        cols[n_dim + 1:end]
    else
        agents
    end

    num_agents = length(cols[n_dim + 1:end])

    _warn_if_file_is_bigger_than_ram(filename_with_extension, "CSV")

    cache = QuiverReaderCache(
        metadata, 
        dimensions_to_cache,
        num_agents
    )

    df = CSV.read(filename_with_extension, DataFrame; types = [fill(Int32, n_dim); fill(Float32, num_agents)], header = header_line)

    return QuiverReader{csv, DataFrame}(
        df,
        filename_with_extension,
        dimensions,
        agents_to_read,
        metadata,
        cache
    )
end

function _quiver_read_df(reader::QuiverReader{csv, DataFrame, N}; kwargs...) where N
    dimensions_to_query = values(kwargs)
    if isempty(dimensions_to_query)
        return reader.reader
    end
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

function _quiver_read(reader::QuiverReader{csv, DataFrame}; dimensions_to_query...)
    cols_of_agents = find_cols_of_agents(reader, Symbol.(names(reader.reader)))
    view_df = _quiver_read_df(reader; dimensions_to_query...)
    return Matrix{Float32}(view_df[:, cols_of_agents])
end

function _quiver_close!(reader::QuiverReader{csv, DataFrame})
    reader.reader = nothing
    return nothing
end