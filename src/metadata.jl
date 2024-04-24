Base.@kwdef mutable struct QuiverMetadata
    # frequency of the time series
    frequency::String = defaukt_frequency()
    # initial date of the time series
    initial_date::Dates.DateTime = default_initial_date()
    # Inform which dimensions represent time
    time_dimension::String
    # number of dimensions columns in file
    num_dimensions::Int
    # unit of the time series
    unit::String = default_unit()
    # Maximum index for each dimension
    maximum_value_of_each_dimension::Vector{Int}
    # version of the file. This is in case anything changes in the future
    version::Int = 1
end

function num_dimensions(metadata::QuiverMetadata)::Int
    return metadata.num_dimensions
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
           "time_dimension: $(metadata.time_dimension)\n" * 
           "num_dimensions: $(metadata.num_dimensions)\n" *
           "unit: $(metadata.unit)\n" *
           "maximum_value_of_each_dimension: $(join(metadata.maximum_value_of_each_dimension, " "))\n" *
           "version: $(metadata.version)\n" * 
           "--- \n"
end

function from_string(str::String)::QuiverMetadata
    lines = split(str, "\n")
    frequency = split(lines[1], ": ")[2]
    initial_date = DateTime(split(lines[2], ": ")[2])
    time_dimension = split(lines[3], ": ")[2]
    num_dimensions = parse(Int, split(lines[4], ": ")[2])
    unit = split(lines[5], ": ")[2]
    maximum_value_of_each_dimension = split(lines[6], ": ")[2]
    version = parse(Int, split(lines[7], ": ")[2])
    return QuiverMetadata(
        frequency,
        initial_date,
        time_dimension,
        num_dimensions,
        unit,
        parse.(Int, split(maximum_value_of_each_dimension, " ")),
        version
    )
end

function to_dict(metadata::QuiverMetadata)::Dict{String, String}
    return Dict(
        "frequency" => metadata.frequency,
        "initial_date" => string(metadata.initial_date),
        "time_dimensions" => metadata.time_dimension,
        "num_dimensions" => "$(metadata.num_dimensions)",
        "unit" => metadata.unit,
        "maximum_value_of_each_dimension" => join(metadata.maximum_value_of_each_dimension, " "),
        "version" => "$(metadata.version)"
    )
end

function from_dict(dict::AbstractDict{String, String})::QuiverMetadata
    return QuiverMetadata(
        dict["frequency"],
        DateTime(dict["initial_date"]),
        dict["time_dimensions"],
        parse(Int, dict["num_dimensions"]),
        dict["unit"],
        parse.(Int, split(dict["maximum_value_of_each_dimension"], " ")),
        parse(Int, dict["version"])
    )
end