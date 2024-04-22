mutable struct QuiverReader{I <: QuiverImplementation, R}
    reader::Union{Nothing, R}
    filename::String
    dimensions::Vector{Symbol}
    agents_to_read::Vector{Symbol}
    metadata::QuiverMetadata
end

"""
    read_file(quiver_reader::QuiverReader, filename::String)::DataFrame

Reads the file and returns the content of the file in a data frame.
"""
function read_file(quiver_reader::QuiverReader, filename::String)::DataFrame
    return _read_file(quiver_reader, filename)
end

"""
    read_metadata(quiver_reader::QuiverReader, filename::String)::QuiverMetadata

Reads the metadata of the file and returns the metadata.
"""
function read_metadata(quiver_reader::QuiverReader, filename::String)::QuiverMetadata
    return _read_metadata(quiver_reader, filename)
end

"""
    read(quiver_reader::QuiverReader, dimensions_to_query::NamedTuple)::Matrix{<:AbstractFloat}

Reads a view of the file and returns the content in a matrix of floats.
"""
function read(reader::QuiverReader, dimensions_to_query::NamedTuple)::Matrix{<:AbstractFloat}
    _assert_dimensions_are_in_file_order(reader, dimensions_to_query)
    return _quiver_read(reader, dimensions_to_query)
end

function _assert_dimensions_are_in_file_order(reader::QuiverReader, dimensions_to_query::NamedTuple)
    keys_dims_to_query = keys(dimensions_to_query)
    for (i, dim) in enumerate(keys_dims_to_query)
        if dim != reader.dimensions[i]
            error("Dimensions must be read in the order of the file. (Expected the order $(reader.dimensions)")
        end
    end
    return nothing
end

function _find_cols_of_agents(reader::QuiverReader, cols::Vector{Symbol})
    return findall((in)(reader.agents_to_read), cols)
end

"""
    close!(reader::QuiverReader)::Nothing

Closes the file reader.
"""
function close!(reader::QuiverReader)
    _quiver_close!(reader)
    return nothing
end