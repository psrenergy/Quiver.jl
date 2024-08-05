mutable struct Writer{I <: Implementation, W}
    writer::W
    filename::String
    metadata::Metadata
    last_dimension_added::Vector{Int}
end

function _build_last_dimension_added!(writer::Writer, dims...)
    for (i, dim) in enumerate(dims)
        writer.last_dimension_added[i] = dim[2]
    end
    return nothing
end

function write!(writer::Writer, data::Vector{T}; dims...) where T <: Real
    validate_dimensions(writer.metadata, dims...)
    _build_last_dimension_added!(writer, dims...)
    _quiver_write!(writer, data)
end

function close!(writer::Writer)
    _quiver_close!(writer)
end