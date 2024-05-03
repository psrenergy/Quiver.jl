function convert(
    filename::String,
    src_implementation::Type{<:QuiverImplementation},
    dst_implementation::Type{<:QuiverImplementation}
)
    if src_implementation == dst_implementation
        error("The source and destination implementations are the same.")
    end

    reader = QuiverReader{src_implementation}(filename)
    writer = QuiverWriter{dst_implementation}(
        filename,
        string.(reader.dimensions),
        string.(reader.agents_to_read),
        reader.metadata.time_dimension,
        reader.metadata.maximum_value_of_each_dimension;
        frequency = reader.metadata.frequency,
        initial_date = reader.metadata.initial_date,
        unit = reader.metadata.unit,
    )

    # The outermost dimension is one of the fastest ways 
    # to iterate over the data.
    outermost_dimension = string(reader.dimensions[1])

    # This code assumes that the first dimension has a minimum of one.
    # This is not entirely true, there could be negative stages.
    for i in 1:max_index(reader, outermost_dimension)
        dimensions_to_query = NamedTuple(
            (Symbol(outermost_dimension) => i,)
        )
        df = _quiver_read_df(reader, dimensions_to_query)
        _quiver_write!(writer, df)
    end

    _quiver_close!(reader)
    _quiver_close!(writer)

    return nothing
end

# From and to a PSRI Open Binary.

# Another thing we should do is a small go program to open the Arrow file and show the header.
# I would love to give a try to this program in go.