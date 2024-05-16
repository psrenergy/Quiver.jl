mutable struct QuiverReader{I <: QuiverImplementation, R}
    reader::Union{Nothing, R}
    filename::String
    dimensions::Vector{Symbol}
    agents_to_read::Vector{Symbol}
    metadata::QuiverMetadata
end

function max_index(reader::QuiverReader, dimension::String)
    index_of_dimension = findfirst(isequal(dimension), string.(reader.dimensions))
    if index_of_dimension === nothing
        error("Dimension $dimension not found in $(reader.dimensions)")
    end
    return reader.metadata.maximum_value_of_each_dimension[index_of_dimension]
end

function read(reader::QuiverReader; dimensions_to_query...)
    _assert_dimensions_are_in_order(reader; dimensions_to_query...)
    return _quiver_read(reader; dimensions_to_query...)
end

function find_cols_of_agents(reader::QuiverReader, cols::Vector{Symbol})
    return findall((in)(reader.agents_to_read), cols)
end

function close!(writer::QuiverReader)
    _quiver_close!(writer)
    return nothing
end