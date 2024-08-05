mutable struct QuiverCSVRowReader
    iterator
    next
end

function Writer{csv}(
    filename::String;
    names_of_dimensions::Vector{String},
    names_of_time_series::Vector{String},
    time_dimension::String,
    maximum_value_of_each_dimension::Vector{Int},
    remove_if_exists::Bool = true,
    kwargs...,
)

    filename_with_extensions = add_extension_to_file(filename, "csv")
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
    print(io, join(metadata.names_of_dimensions, ",") * "," * join(metadata.names_of_time_series, ",") * "\n")
    last_dimension_added = zeros(Int, metadata.number_of_dimensions)

    writer = Quiver.Writer{csv, typeof(io)}(
        io,
        filename,
        metadata,
        last_dimension_added
    )

    to_toml(metadata, "$filename.toml")

    return writer
end

function _quiver_write!(writer::Quiver.Writer{csv}, data::Vector{T}) where T <: Real
    # The last dimension added is calculated in the abstract implementation
    print(writer.writer, join(writer.last_dimension_added, ","), ",", join(data, ","), "\n")
    return nothing
end

function _quiver_close!(writer::Quiver.Writer{csv})
    close(writer.writer)
    return nothing
end

function Reader{csv}(
    filename::String;
)

    filename_with_extensions = add_extension_to_file(filename, "csv")
    if !isfile(filename_with_extensions)
        throw(ArgumentError("File $filename_with_extensions does not exist"))
    end

    metadata = from_toml("$filename.toml")

    rows = CSV.Rows(filename_with_extensions; types = [fill(Int32, metadata.number_of_dimensions); fill(Float32, metadata.number_of_time_series)])

    last_dimension_read = zeros(Int, metadata.number_of_dimensions)
    data = zeros(Float32, metadata.number_of_time_series)

    next = iterate(rows)
    (row, state) = next

    row_reader = QuiverCSVRowReader(rows, next)

    reader = Quiver.Reader{csv, typeof(row_reader)}(
        row_reader,
        filename,
        metadata,
        last_dimension_read,
        data
    )

    return reader
end

function _quiver_next_dimension!(reader::Quiver.Reader{csv})
    if reader.reader.next === nothing
        error("No more data to read")
        return nothing
    end
    (row, state) = reader.reader.next
    for (i, dim) in enumerate(reader.metadata.names_of_dimensions)
        reader.last_dimension_read[i] = row[Symbol(dim)]
    end
    for (i, ts) in enumerate(reader.metadata.names_of_time_series)
        reader.data[i] = row[Symbol(ts)]
    end
    next = iterate(reader.reader.iterator, state)
    reader.reader.next = next
    return nothing
end

function _quiver_goto!(reader::Quiver.Reader{csv}, dims...)
    error("_quiver_goto! not implemented for csv")
    return nothing
end

function _quiver_close!(reader::Quiver.Reader{csv})
    return nothing
end