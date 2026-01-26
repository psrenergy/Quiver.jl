function quiver_read!(file_pointer::FilePointer; dims...)
    validate_dims(file_pointer.metadata; dims...)

    position = calculate_file_position(file_pointer.metadata; dims...)
    go_to_position!(file_pointer, position)

    data = zeros(Float64, file_pointer.metadata.number_of_labels)
    read!(file_pointer.io, data)

    return data
end

function quiver_write!(file_pointer::FilePointer, data::Vector{Float64}; dims...)
    validate_dims(file_pointer.metadata; dims...)
    validate_data_length(data, file_pointer.metadata)

    position = calculate_file_position(file_pointer.metadata; dims...)
    go_to_position!(file_pointer, position)

    # TODO: improve with @inbounds
    for i in eachindex(data)
        write(file_pointer.io, data[i])
    end

    return nothing
end

function calculate_file_position(metadata::Metadata; dims...)
    position = 0
    for (i, dim) in enumerate(metadata.dimensions)
        # TODO: improve with @inbounds
        position += (dims[dim] - min_value_per_dimension(metadata)[i]) * prod(metadata.dimension_sizes[i+1:end])
    end
    position *= metadata.number_of_labels
    position *= sizeof(Float64)
    return position
end

function go_to_position!(file_pointer::FilePointer, next_position::Int)
    validate_not_negative(next_position, "file position")
    validate_file_is_open(file_pointer)

    current_position = position(file_pointer.io)
    if next_position >= current_position
        skip(file_pointer.io, next_position - current_position)
    else
        seek(file_pointer.io, next_position)
    end

    return nothing
end
