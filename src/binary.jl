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

function _calculate_position_in_file(metadata::Quiver.Metadata, dims...)
    space_of_a_row = 4 * metadata.number_of_time_series
    position = 0
    for i in 1:metadata.number_of_dimensions - 1
        position += (dims[i] - 1) * prod(metadata.maximum_value_of_each_dimension[1+i:end])
    end
    position += (dims[end] - 1)
    position *= space_of_a_row
    return position
end

function _quiver_write!(writer::Quiver.Writer{binary}, data::Vector{T}) where T <: Real
    # The last dimension added is calculated in the abstract implementation
    pos = _calculate_position_in_file(writer.metadata, writer.last_dimension_added...)
    seek(writer.writer, pos)
    @inbounds for i in eachindex(data)
        write(writer.writer, Float32(data[i]))
    end
    return nothing
end

function _quiver_close!(writer::Quiver.Writer{binary})
    close(writer.writer)
    return nothing
end

mutable struct BinaryReader
    io
    current_position::Int
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

    binary_reader = BinaryReader(io, 0)

    reader = Quiver.Reader{binary, typeof(binary_reader)}(
        binary_reader,
        filename,
        metadata,
        last_dimension_read,
        data
    )

    return reader
end

function _quiver_goto!(reader::Quiver.Reader{binary})
    position = _calculate_position_in_file(reader.metadata, reader.last_dimension_read...)
    seek(reader.reader.io, position)
    reader.reader.current_position = position
    read!(reader.reader.io, reader.data)
    return nothing
end

function _quiver_next_dimension!(reader::Quiver.Reader{binary})
    error("Not implemented")
    return nothing
end

function _quiver_close!(reader::Quiver.Reader{binary})
    close(reader.reader.io)
    return nothing
end