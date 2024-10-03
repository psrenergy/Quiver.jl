function validate_dimensions(metadata::Metadata, dims...)
    if length(dims) != metadata.number_of_dimensions
        throw(ArgumentError("The number of dimensions is incorrect"))
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

function create_empty_time_series(
    filename::AbstractString,
    impl::Type{<:Implementation},
    dimensions::Vector{String},
    labels::Vector{String},
    time_dimension::String,
    dimension_size::Vector{Int},
    initial_date::Union{String, DateTime} = "",
    unit::String = "",
    digits::Int = 6,
)
    file_created = 0
    filename_with_extensions = add_extension_to_file(filename, file_extension(impl))
    if isfile(filename_with_extensions)
        return file_created
    end

    @warn("Creating empty time series file at $(filename).")
    file_created = 1
    data = zeros(Float64, length(labels), reverse(dimension_size)...)
    array_to_file(
        filename,
        data,
        impl;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date,
        unit,
        digits,
    )
    return file_created
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
