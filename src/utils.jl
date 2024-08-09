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

function _assert_dimensions_are_in_order(reader_or_writer; dims...)
    keys_dims_to_query = keys(dims)
    for (i, dim) in enumerate(dims)
        if dim != Symbol.(reader_or_writer.dimensions[i])
            error("Dimensions must be read in the order of the file. (Expected the order $(reader_or_writer.dimensions)")
        end
    end
    return nothing
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
