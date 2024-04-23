Base.@kwdef mutable struct QuiverMetadata
    # number of dimensions columns in file
    dimension_names::Vector{String}
    # frequency of the time series
    frequency::String = defaukt_frequency()
    # initial date of the time series
    initial_date::Dates.DateTime = default_initial_date()
    # unit of the time series
    unit::String = default_unit()
    # Inform which dimensions represent time
    time_dimensions::Vector{String}
    # Maximum index for each dimension
    maximum_value_of_each_dimension::Vector{Int}
    # version of the file. This is in case anything changes in the future
    const version::String = "1"
end

function num_dimensions(metadata::QuiverMetadata)::Int
    return length(metadata.dimension_names)
end

function max_index(metadata::QuiverMetadata, dimension_name::String)::Int
    index = findfirst(metadata.dimension_names .== dimension_name)
    if index === nothing
        error("Dimension $dimension_name not found in metadata")
    end
    return metadata.maximum_value_of_each_dimension[index]
end

function default_frequency()::String
    return "M"
end

function default_initial_date()::Dates.DateTime
    return DateTime(1900)
end

function default_unit()::String
    return ""
end

function to_string(metadata::QuiverMetadata)::String
    return "frequency: $(metadata.frequency)\n" *
           "initial_date: $(metadata.initial_date)\n" *
           "unit: $(metadata.unit)\n" *
           "dimension_names: $(join(metadata.dimension_names, " "))\n" *
           "maximum_value_of_each_dimension: $(join(metadata.maximum_value_of_each_dimension, " "))\n" *
           "time_dimensions: $(join(metadata.time_dimensions, " "))\n" * 
           "version: $(metadata.version)\n" * 
           "--- \n"
end

function from_string(str::String)::QuiverMetadata
    lines = split(str, "\n")
    frequency = split(lines[1], ": ")[2]
    initial_date = DateTime(split(lines[2], ": ")[2])
    unit = split(lines[3], ": ")[2]
    dimension_names = split(lines[4], ": ")[2]
    maximum_value_of_each_dimension = split(lines[5], ": ")[2]
    time_dimensions = split(lines[6], ": ")[2]
    version = split(lines[7], ": ")[2]
    return QuiverMetadata(
        split(dimension_names, " "),
        frequency,
        initial_date,
        unit,
        split(time_dimensions, " "),
        parse.(Int, split(maximum_value_of_each_dimension, " ")),
        version
    )
end

function to_dict(metadata::QuiverMetadata)::Dict{String, String}
    return Dict(
        "frequency" => metadata.frequency,
        "initial_date" => string(metadata.initial_date),
        "unit" => metadata.unit,
        "dimension_names" => join(metadata.dimension_names, " "),
        "maximum_value_of_each_dimension" => join(metadata.maximum_value_of_each_dimension, " "),
        "time_dimensions" => join(metadata.time_dimensions, " "),
        "version" => metadata.version
    )
end

function from_dict(dict::AbstractDict{String, String})::QuiverMetadata
    return QuiverMetadata(
        split(dict["dimension_names"], " "),
        dict["frequency"],
        DateTime(dict["initial_date"]),
        dict["unit"],
        split(dict["time_dimensions"], " "),
        parse.(Int, split(dict["maximum_value_of_each_dimension"], " ")),
        dict["version"]
    )
end