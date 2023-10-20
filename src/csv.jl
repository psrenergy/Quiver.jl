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

    # This one is to put the correct types
    rows = CSV.Rows(filename_with_extension; types = [fill(Int32, meta_data.num_dimensions); fill(Float32, num_agents)])

    return QuiverReader{csv, CSV.Rows}(
        rows,
        dimensions,
        agents_to_read,
        meta_data
    )
end

function _quiver_read(reader::QuiverReader{csv, CSV.Rows}, dimensions_to_query::NamedTuple)
    filename = reader.reader.name
    cols_of_agents = find_cols_of_agents(reader, Symbol.(reader.reader.names))
    selected_rows = Vector{Vector{Float32}}(undef, 0)
    # TODO this here could be smarter
    # We could check what is the last row searched and simply continue through this 
    # same iterator.
    # Here we are starting again and iterating through the whole file every time
    for row in CSV.Rows(filename; types = reader.reader.ctx.types)
        is_row_selected = true
        for (i, dim_to_query) in enumerate(dimensions_to_query)
            if row[i] != dim_to_query
                is_row_selected = false
                break
            end
        end
        if is_row_selected
            # TODO it is possible to make a smarter collect that only build the appropriate
            # sized vector
            push!(selected_rows, collect(Float32, row)[cols_of_agents])
        end
    end

    return gather_selected_rows(selected_rows)
end

function _quiver_close!(reader::QuiverReader{csv, CSV.Rows})
    reader.reader = nothing
    return nothing
end

function gather_selected_rows(selected_rows::Vector{Vector{Float32}})
    n_rows = length(selected_rows)
    n_cols = length(selected_rows[1])
    m = Matrix{Float32}(undef, n_rows, n_cols)
    # TODO check if the other order is better for performances
    for i in axes(m, 1), j in axes(m, 2)
        m[i, j] = selected_rows[i][j]
    end
    return m
end