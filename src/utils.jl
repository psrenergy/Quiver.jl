function rm_if_exists(filename::AbstractString, remove_if_exists::Bool)
    if isfile(filename)
        if remove_if_exists
            rm(filename; force = true)
        else
            error("File $filename already exists.")
        end
    end
end

function _assert_dimensions_are_in_order(reader_or_writer; dimensions_to_query...)
    keys_dims_to_query = keys(dimensions_to_query)
    for (i, dim) in enumerate(keys_dims_to_query)
        if dim != reader_or_writer.dimensions[i]
            error("Dimensions must be read in the order of the file. (Expected the order $(reader.dimensions)")
        end
    end
    return nothing
end

function _create_quiver_empty_df(
    dimensions::Vector{String}, 
    agents::Vector{String}
)
    quiver_empty_df = DataFrames.DataFrame()
    for dim in dimensions
        quiver_empty_df[!, Symbol(dim)] = Int32[]
    end
    for agent in agents
        quiver_empty_df[!, Symbol(agent)] = Float32[]
    end
    return quiver_empty_df
end

function add_extension_to_file(filename::AbstractString, ext::AbstractString)
    # This regex is to check if a file has an extension
    # https://stackoverflow.com/questions/22863973/regex-check-if-a-file-has-any-extension
    if occursin(r"^.*\.[^\\]+$", filename)
        error("Filename $filename already has an extension.")
    end
    return "$filename.$ext"
end

function default_last_dimension_added(dimensions::Vector)
    return fill(Int32(0), length(dimensions))
end

function _warn_if_file_is_bigger_than_ram(filename::AbstractString, implementation::String)
    size_of_file_mb = filesize(filename) / 2^20 |> trunc
    freememory_mb = Sys.free_memory() / 2^20 |> trunc
    if size_of_file_mb / 2 > freememory_mb
        @warn(
            "The Quiver implementation for $implementation is not optimized for large files. " *
            "The file has $(size_of_file_mb) MB and the available memory is $(freememory_mb) MB. " *
            "The program might run out of memory. Please consider using the Arrow implementation instead."
        )
    end
    return nothing
end