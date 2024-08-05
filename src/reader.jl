mutable struct Reader{I <: Implementation, R}
    reader::R
    filename::String
    metadata::Metadata
    last_dimension_read::Vector{Int}
    data::Vector{Float32}
end

function goto(reader::Reader; dims...)
    _quiver_goto!(reader; dims...)
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