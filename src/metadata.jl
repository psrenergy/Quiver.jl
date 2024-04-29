mutable struct QuiverMetadata
    # frequency of the time series
    frequency::String
    # initial date of the time series
    initial_date::Dates.DateTime
    # Inform which dimensions represent time
    time_dimension::String
    # number of dimensions columns in file
    num_dimensions::Int
    # unit of the time series
    unit::String
    # Maximum index for each dimension
    maximum_value_of_each_dimension::Vector{Int}
    # version of the file. This is in case anything changes in the future
    version::Int

    function QuiverMetadata(;
        time_dimension::String,
        frequency::String = default_frequency(),
        initial_date::Dates.DateTime = default_initial_date(),
        num_dimensions::Int,
        unit::String = default_unit(),
        maximum_value_of_each_dimension::Vector{Int},
        version::Int = 1
    )

        if num_dimensions != length(maximum_value_of_each_dimension)
            error("The number of dimensions must be equal to the length of maximum_value_of_each_dimension")
        end

        new(
            frequency,
            initial_date,
            time_dimension,
            num_dimensions,
            unit,
            maximum_value_of_each_dimension,
            version
        )
    end	
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
    return QuiverMetadata(;
        frequency = string(frequency),
        initial_date,
        time_dimension = string(time_dimension),
        num_dimensions,
        unit = string(unit),
        maximum_value_of_each_dimension = parse.(Int, split(maximum_value_of_each_dimension, " ")),
        version
    )
end

function to_dict(metadata::QuiverMetadata)::Dict{String, String}
    return Dict(
        "frequency" => metadata.frequency,
        "initial_date" => string(metadata.initial_date),
        "time_dimension" => metadata.time_dimension,
        "num_dimensions" => "$(metadata.num_dimensions)",
        "unit" => metadata.unit,
        "maximum_value_of_each_dimension" => join(metadata.maximum_value_of_each_dimension, " "),
        "version" => "$(metadata.version)"
    )
end

function from_dict(dict::AbstractDict{String, String})::QuiverMetadata
    return QuiverMetadata(;
        frequency = dict["frequency"],
        initial_date = DateTime(dict["initial_date"]),
        time_dimension = dict["time_dimension"],
        num_dimensions = parse(Int, dict["num_dimensions"]),
        unit = dict["unit"],
        maximum_value_of_each_dimension = parse.(Int, split(dict["maximum_value_of_each_dimension"], " ")),
        version = parse(Int, dict["version"])
    )
end