# TODO enum stage type

mutable struct QuiverMetadata
    num_dimensions::Int
    stage_type::Union{Nothing, String}
    initial_date::Union{Nothing, Dates.DateTime}
    unit::Union{Nothing, String}
end

function to_dict(metadata::QuiverMetadata)
    dict = Dict{String, String}()
    for f in fieldnames(QuiverMetadata)
        if getfield(metadata, f) !== nothing
            dict[string(f)] = string(getfield(metadata, f))
        end
    end
    return dict
end

function from_file(metadata_file::AbstractString)
    @assert isfile(metadata_file)
    return from_dict(TOML.parsefile(metadata_file))
end

function from_dict(dict::Dict{String, Any})
    metadata = QuiverMetadata(
        parse(Int, dict["num_dimensions"]),
        get(dict, "stage_type", nothing),
        get(dict, "initial_date", nothing),
        get(dict, "unit", nothing)
    )
    return metadata
end

function to_toml(filename::String, metadata::QuiverMetadata)
    @assert !isfile(filename)
    open(filename, "w") do io
        TOML.print(io, to_dict(metadata))
    end
    return filename
end