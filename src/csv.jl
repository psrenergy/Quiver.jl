file_extension(::Type{csv}) = "csv"

mutable struct QuiverCSVRowReader
    io
    iterator
    next
end

function Writer{csv}(
    filename::String;
    dimensions::Vector{String},
    labels::Vector{String},
    time_dimension::String,
    dimension_size::Vector{Int},
    remove_if_exists::Bool = true,
    kwargs...,
)

    filename_with_extensions = add_extension_to_file(filename, file_extension(csv))
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
    print(io, join(metadata.dimensions, ",") * "," * join(metadata.labels, ",") * "\n")
    last_dimension_added = zeros(Int, metadata.number_of_dimensions)

    writer = Quiver.Writer{csv}(
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
    labels_to_read::Vector{String} = String[],
    carrousel::Bool = false,
)
    # Note to the future https://discourse.julialang.org/t/closing-files-when-using-csv-rows/29169/2
    # CSV.jl mmaps the file and keeps it open until the rows object is garbage collected
    # To avoid this we can follow the recommendation in the link above

    filename_with_extensions = add_extension_to_file(filename, file_extension(csv))
    if !isfile(filename_with_extensions)
        throw(ArgumentError("File $filename_with_extensions does not exist"))
    end

    metadata = from_toml("$filename.toml")

    io = open(filename_with_extensions, "r")

    rows = CSV.Rows(
        io; 
        types = [fill(Int32, metadata.number_of_dimensions); fill(Float32, metadata.number_of_time_series)],
        buffer_in_memory = true,
        reusebuffer = true,
    )

    last_dimension_read = zeros(Int, metadata.number_of_dimensions)

    next = iterate(rows)
    (row, state) = next

    
    reader = try
        row_reader = QuiverCSVRowReader(io, rows, next)
        Quiver.Reader{csv}(
            row_reader,
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

function _quiver_next_dimension!(reader::Quiver.Reader{csv})
    if reader.reader.next === nothing
        error("No more data to read")
        return nothing
    end
    (row, state) = reader.reader.next
    for (i, dim) in enumerate(reader.metadata.dimensions)
        reader.last_dimension_read[i] = row[dim]
    end
    for (i, ts) in enumerate(reader.metadata.labels)
        reader.all_labels_data_cache[i] = row[Symbol(ts)]
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
    close(reader.reader.io)
    return nothing
end

function convert(
    filepath::String,
    from::Type{csv},
    to::Type{impl};
    destination_directory::String = dirname(filepath),
) where impl <: Implementation
    reader = Quiver.Reader{from}(filepath)
    metadata = reader.metadata
    filename = basename(filepath)
    destination_filepath = joinpath(destination_directory, filename)
    writer = Quiver.Writer{to}(
        destination_filepath;
        dimensions = String.(metadata.dimensions),
        labels = metadata.labels,
        time_dimension = String(metadata.time_dimension),
        dimension_size = metadata.dimension_size,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
    )

    while reader.reader.next !== nothing
        Quiver.next_dimension!(reader)
        dim_kwargs = OrderedDict(Symbol.(metadata.dimensions) .=> reader.last_dimension_read)
        Quiver.write!(writer, reader.data; dim_kwargs...)
    end

    Quiver.close!(reader)
    Quiver.close!(writer)

    return nothing
end
