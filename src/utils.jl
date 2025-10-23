function validate_dimensions(metadata::Metadata, dims...)
    if length(dims) != metadata.number_of_dimensions
        throw(ArgumentError("The number of dimensions is incorrect"))
    end
    return nothing
end

function validate_data_length(metadata::Metadata, data::Vector{T}) where {T <: Real}
    if length(data) != length(metadata.labels)
        throw(ArgumentError("The length of the data is incorrect"))
    end
    return nothing
end

function rm_if_exists(filename::AbstractString, remove_if_exists::Bool)
    if isfile(filename)
        if remove_if_exists
            rm(filename; force = true)
        else
            error("File $filename already exists.")
        end
    end
end

function add_extension_to_file(filename::AbstractString, ext::AbstractString)
    # This regex is to check if a file has an extension
    # https://stackoverflow.com/questions/22863973/regex-check-if-a-file-has-any-extension
    file_extension_regex = if Sys.iswindows()
        r"^.*\.[^\\]+$"
    else
        r"^.*\.[^/]+$"
    end
    if occursin(file_extension_regex, filename)
        error("Filename $filename already has an extension.")
    end
    return "$filename.$ext"
end

function next_dim!(
    current_dimensions::Vector{Int},
    max_size_dimensions::Vector{Int},
    dimension_offset::Vector{Int},
)
    for i in length(current_dimensions):-1:1
        max_value = dimension_offset[i] + max_size_dimensions[i] - 1
        if current_dimensions[i] < max_value
            current_dimensions[i] += 1
            for j in i+1:length(current_dimensions)
                current_dimensions[j] = dimension_offset[j]
            end
            return
        end
    end
    return
end

# Backward compatibility version
function next_dim!(
    current_dimensions::Vector{Int},
    max_size_dimensions::Vector{Int},
)
    dimension_offset = ones(Int, length(max_size_dimensions))
    return next_dim!(current_dimensions, max_size_dimensions, dimension_offset)
end

function first_position!(
    max_size_dimensions::Vector{Int},
    dimension_offset::Vector{Int},
)
    dims = copy(dimension_offset)
    dims[end] = dimension_offset[end] - 1
    return dims
end

# Backward compatibility version
function first_position!(
    max_size_dimensions::Vector{Int},
)
    dimension_offset = ones(Int, length(max_size_dimensions))
    return first_position!(max_size_dimensions, dimension_offset)
end
