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
