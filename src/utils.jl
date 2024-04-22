function create_quiver_empty_df(
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
    if occursin(r"\.", filename)
        error("Filename $filename already has an extension.")
    end
    return "$filename.$ext"
end

function default_last_dimension_added(dimensions::Vector)
    return fill(Int32(-10000), length(dimensions))
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