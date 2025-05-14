function compare_files(
    filename1::String,
    filename2::String,
    implementation::Type{I};
    labels_to_read::Vector{String} = String[],
    atol::Real = DEFAULT_ATOL,
    rtol::Real = DEFAULT_RTOL,
)::Bool where {I <: Implementation}
    data1, metadata1 = file_to_array(filename1, implementation; labels_to_read)
    data2, metadata2 = file_to_array(filename2, implementation; labels_to_read)

    if metadata1 != metadata2
        return false
    end

    if compare_data(data1, data2; atol = atol, rtol = rtol) == false
        return false
    end

    return true
end

function compare(
    filename::String,
    implementation::Type{I};
    atol::Real = DEFAULT_ATOL,
    rtol::Real = DEFAULT_RTOL,
    data::Union{Array, Nothing} = nothing,
    kwargs...,
)::Bool where {I <: Implementation}
    quiver_data, metadata = file_to_array(filename, implementation)

    if compare_metadata(metadata; kwargs...) == false
        return false
    end

    if compare_data(quiver_data, data; atol = atol, rtol = rtol) == false
        return false
    end

    return true
end
