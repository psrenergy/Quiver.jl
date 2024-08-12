file_extension(::Type{binary}) = "quiv"

function Writer{binary}(
    filename::String;
    dimensions::Vector{String},
    labels::Vector{String},
    time_dimension::String,
    dimension_size::Vector{Int},
    remove_if_exists::Bool = true,
    kwargs...,
)

    filename_with_extensions = add_extension_to_file(filename, file_extension(binary))
    rm_if_exists(filename_with_extensions, remove_if_exists)

    metadata = Quiver.Metadata(;
        dimensions = dimensions,
        time_dimension = time_dimension,
        dimension_size = dimension_size,
        labels = labels,
        kwargs...
    )

    # Open the file and write the header
    io = open(filename_with_extensions, "w")
    last_dimension_added = zeros(Int, metadata.number_of_dimensions)

    writer = Quiver.Writer{binary}(
        io,
        filename,
        metadata,
        last_dimension_added
    )

    to_toml(metadata, "$filename.toml")

    return writer
end

function performant_product_from_index_i_to_j(arr::Vector{Int}, i::Int, j::Int)
    result = 1
    @inbounds for k in i:j
        result *= arr[k]
    end
    return result
end

function _calculate_position_in_file(metadata::Quiver.Metadata, dims...)
    space_of_a_row = 4 * metadata.number_of_time_series
    position = 0
    for i in 1:metadata.number_of_dimensions - 1
        position += (dims[i] - 1) * performant_product_from_index_i_to_j(
            metadata.dimension_size, 
            i + 1, 
            metadata.number_of_dimensions
        )
    end
    position += (dims[end] - 1)
    position *= space_of_a_row
    return position
end

function _quiver_write!(writer::Quiver.Writer{binary}, data::Vector{T}) where T <: Real
    # The last dimension added is calculated in the abstract implementation
    next_pos = _calculate_position_in_file(writer.metadata, writer.last_dimension_added...)
    # Check if we need to seek a new position or write directly in the io
    # This is absolutely necessary for performance in the binary operation
    current_pos = position(writer.writer)
    if current_pos > next_pos
        seek(writer.writer, next_pos)
    elseif current_pos < next_pos
        space_of_a_row = 4 * writer.metadata.number_of_time_series
        number_of_empty_rows = (next_pos - current_pos) / space_of_a_row
        for _ in 1:number_of_empty_rows
            @inbounds for i in eachindex(data)
                write(writer.writer, NaN32)
            end
        end
    end
    @inbounds for i in eachindex(data)
        write(writer.writer, Float32(data[i]))
    end
    return nothing
end

function _quiver_close!(writer::Quiver.Writer{binary})
    close(writer.writer)
    return nothing
end

function Reader{binary}(
    filename::String;
    labels_to_read::Vector{String} = String[],
    carrousel::Bool = false,
)

    filename_with_extensions = add_extension_to_file(filename, file_extension(binary))
    if !isfile(filename_with_extensions)
        throw(ArgumentError("File $filename_with_extensions does not exist"))
    end

    metadata = from_toml("$filename.toml")

    io = open(filename_with_extensions, "r")

    last_dimension_read = zeros(Int, metadata.number_of_dimensions)

    reader = try 
        Quiver.Reader{binary}(
            io,
            filename,
            metadata,
            last_dimension_read;
            labels_to_read = isempty(labels_to_read) ? metadata.labels : labels_to_read,
            carrousel = carrousel,
        )
    catch e
        close(io)
        rethrow(e)
    end

    return reader
end

function _quiver_goto!(reader::Quiver.Reader{binary})
    next_pos = _calculate_position_in_file(reader.metadata, reader.last_dimension_read...)
    # Check if we need to seek a new position or write directly in the io
    # This is absolutely necessary for performance in the binary operation
    current_pos = position(reader.reader)
    if next_pos >= current_pos
        skip(reader.reader, next_pos - current_pos)
    else
        seek(reader.reader, next_pos)
    end
    read!(reader.reader, reader.all_labels_data_cache)
    return nothing
end

function _quiver_next_dimension!(reader::Quiver.Reader{binary})
    error("Not implemented")
    return nothing
end

function _quiver_close!(reader::Quiver.Reader{binary})
    close(reader.reader)
    return nothing
end

function convert(
    filename::String,
    from::Type{binary},
    to::Type{impl},
) where impl <: Implementation
    reader = Quiver.Reader{from}(filename)
    metadata = reader.metadata
    writer = Quiver.Writer{to}(
        filename;
        dimensions = String.(metadata.dimensions),
        labels = metadata.labels,
        time_dimension = String(metadata.time_dimension),
        dimension_size = metadata.dimension_size,
        initial_date = metadata.initial_date,
    )

    for dims in Iterators.product([1:size for size in reverse(metadata.dimension_size)]...)
        dim_kwargs = OrderedDict(Symbol.(metadata.dimensions) .=> reverse(dims))
        Quiver.goto!(reader; dim_kwargs...)
        if all(isnan.(reader.data))
            continue
        end
        Quiver.write!(writer, reader.data; dim_kwargs...)
    end

    Quiver.close!(reader)
    Quiver.close!(writer)

    return nothing
end
