mutable struct Reader{I <: Implementation, R}
    reader::R
    filename::String
    metadata::Metadata
    last_dimension_read::Vector{Int}
    data::Vector{Float32}
    function Reader{I}(
        reader::R, 
        filename::String, 
        metadata::Metadata, 
        last_dimension_read::Vector{Int},
        data::Vector{Float32}
    ) where {I, R}
        reader = new{I, R}(reader, filename, metadata, last_dimension_read, data)
        finalizer(Quiver.close!, reader)
        return reader
    end
end

function _build_last_dimension_read!(reader::Reader; dims...)
    for (i, dim) in enumerate(dims)
        reader.last_dimension_read[i] = dim[2]
    end
    return nothing
end

function goto!(reader::Reader; dims...)
    _build_last_dimension_read!(reader; dims...)
    _quiver_goto!(reader)
    return reader.data
end

function next_dimension!(reader::Reader)
    _quiver_next_dimension!(reader)
    return reader.data
end

function close!(reader::Reader)
    _quiver_close!(reader)
    return nothing
end