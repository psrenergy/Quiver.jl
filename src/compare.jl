function compare_field(field_value::T1, value::T2)::Bool where {T1, T2}
    if field_value isa Symbol
        return string(field_value) == value
    elseif field_value isa AbstractArray
        if size(field_value) != size(value)
            return false
        end

        if eltype(field_value) == Symbol
            return all(string.(field_value) .== value)
        end
    end
    return field_value == value
end

function compare_metadata(metadata::Metadata; kwargs...)::Bool
    for (key, value) in kwargs
        field_value = getfield(metadata, key)
        if compare_field(field_value, value) == false
            @error("Field $key is $field_value, expected $value")
            return false
        end
    end

    return true
end

function compare_data(
    data1::Array,
    data2::Array;
    atol::Real = DEFAULT_ATOL,
    rtol::Real = DEFAULT_RTOL,
)::Bool
    if size(data1) != size(data2)
        return false
    end

    for i in eachindex(data1)
        if isnan(data1[i]) && isnan(data2[i])
            continue
        elseif isnan(data1[i]) && !isnan(data2[i])
            return false
        elseif !isnan(data1[i]) && isnan(data2[i])
            return false
        end
        if !isapprox(data1[i], data2[i]; atol = atol, rtol = rtol)
            return false
        end
    end

    return true
end

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
