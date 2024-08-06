function Writer{binary}(
    filename::String;
    names_of_dimensions::Vector{String},
    names_of_time_series::Vector{String},
    time_dimension::String,
    maximum_value_of_each_dimension::Vector{Int},
    remove_if_exists::Bool = true,
    kwargs...,
)

    filename_with_extensions = add_extension_to_file(filename, "quiv")
    rm_if_exists(filename_with_extensions, remove_if_exists)

    metadata = Quiver.Metadata(;
        names_of_dimensions = names_of_dimensions,
        time_dimension = time_dimension,
        maximum_value_of_each_dimension = maximum_value_of_each_dimension,
        names_of_time_series = names_of_time_series,
        kwargs...
    )

    # Open the file and write the header
    io = open(filename_with_extensions, "w")
    last_dimension_added = zeros(Int, metadata.number_of_dimensions)

    writer = Quiver.Writer{binary, typeof(io)}(
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
            metadata.maximum_value_of_each_dimension, 
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
    if current_pos != next_pos
        seek(writer.writer, next_pos)
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
)

    filename_with_extensions = add_extension_to_file(filename, "quiv")
    if !isfile(filename_with_extensions)
        throw(ArgumentError("File $filename_with_extensions does not exist"))
    end

    metadata = from_toml("$filename.toml")

    io = open(filename_with_extensions, "r")

    last_dimension_read = zeros(Int, metadata.number_of_dimensions)
    data = zeros(Float32, metadata.number_of_time_series)

    reader = Quiver.Reader{binary, typeof(io)}(
        io,
        filename,
        metadata,
        last_dimension_read,
        data
    )

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
    read!(reader.reader, reader.data)
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