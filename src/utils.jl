function rm_if_exists(filename::AbstractString, remove_if_exists::Bool)
    if isfile(filename)
        if remove_if_exists
            rm(filename; force = true)
        else
            error("File $filename already exists.")
        end
    end
end

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

function default_last_dimension_added(dimensions::Vector{String})
    return fill(Int32(-10000), length(dimensions))
end