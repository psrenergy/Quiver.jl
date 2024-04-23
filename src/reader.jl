mutable struct QuiverReader{I <: QuiverImplementation, R}
    reader::Union{Nothing, R}
    filename::String
    dimensions::Vector{Symbol}
    agents_to_read::Vector{Symbol}
    metadata::QuiverMetadata
end

function max_index(reader::QuiverReader, dimension::String)
    return max_index(reader.metadata, dimension)
end

function read(reader::QuiverReader, dimensions_to_query::NamedTuple)
    assert_dimensions_are_in_order(reader, dimensions_to_query)
    return _quiver_read(reader, dimensions_to_query)
end

function assert_dimensions_are_in_order(reader::QuiverReader, dimensions_to_query::NamedTuple)
    keys_dims_to_query = keys(dimensions_to_query)
    for (i, dim) in enumerate(keys_dims_to_query)
        if dim != reader.dimensions[i]
            error("Dimensions must be read in the order of the file. (Expected the order $(reader.dimensions)")
        end
    end
    return nothing
end

function find_cols_of_agents(reader::QuiverReader, cols::Vector{Symbol})
    return findall((in)(reader.agents_to_read), cols)
end

function close!(writer::QuiverReader)
    _quiver_close!(writer)
    return nothing
end