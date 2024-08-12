mutable struct Reader{I <: Implementation, R}
    reader::R
    filename::String
    metadata::Metadata
    last_dimension_read::Vector{Int}
    all_labels_data_cache::Vector{Float32}
    data::Vector{Float32}
    labels_to_read::Vector{String}
    indices_of_labels_to_read::Vector{Int}
    carrousel::Bool
    function Reader{I}(
        reader::R, 
        filename::String, 
        metadata::Metadata,
        last_dimension_read::Vector{Int};
        labels_to_read::Vector{String} = metadata.labels,
        carrousel::Bool = false,
    ) where {I, R}

        # Argument validations
        if length(labels_to_read) == 0
            throw(ArgumentError("labels_to_read cannot be empty"))
        end

        # Find the indices of the labels to read
        indices_of_labels_to_read = Vector{Int}(undef, length(labels_to_read))
        for (i, label) in enumerate(labels_to_read)
            index = findfirst(x -> x == label, metadata.labels)
            if index === nothing
                throw(ArgumentError("Label $label not found in metadata"))
            end
            indices_of_labels_to_read[i] = index
        end

        # Fill the buffer cache and data with NaNs
        all_labels_data_cache = fill(NaN32, length(metadata.labels))
        data = fill(NaN32, length(labels_to_read))

        reader = new{I, R}(
            reader, 
            filename, 
            metadata, 
            last_dimension_read, 
            all_labels_data_cache,
            data,
            labels_to_read,
            indices_of_labels_to_read,
            carrousel,
        )
        finalizer(Quiver.close!, reader)
        return reader
    end
end

function _build_last_dimension_read!(reader::Reader; dims...)
    for (i, dim) in enumerate(reader.metadata.dimensions)
        if reader.carrousel
            reader.last_dimension_read[i] = mod1(dims[dim], reader.metadata.dimension_size[i])
        else
            reader.last_dimension_read[i] = dims[dim]
        end
    end
    return nothing
end

function _move_data_from_buffer_cache_to_data!(reader::Reader)
    @inbounds for (i, index) in enumerate(reader.indices_of_labels_to_read)
        reader.data[i] = reader.all_labels_data_cache[index]
    end
    return nothing
end

function goto!(reader::Reader; dims...)
    validate_dimensions(reader.metadata, dims...)
    _build_last_dimension_read!(reader; dims...)
    _quiver_goto!(reader)
    _move_data_from_buffer_cache_to_data!(reader)
    return reader.data
end

function next_dimension!(reader::Reader)
    _quiver_next_dimension!(reader)
    _move_data_from_buffer_cache_to_data!(reader)
    return reader.data
end

function max_index(reader::Reader, dimension::String)
    index = findfirst(isequal(dimension), reader.metadata.dimensions)
    if index === nothing
        throw(ArgumentError("Dimension $dimension not found in metadata"))
    end
    return reader.metadata.dimension_size[index]
end

function close!(reader::Reader)
    _quiver_close!(reader)
    return nothing
end