mutable struct Metadata
    # frequency of the time series
    frequency::String
    # initial date of the time series
    initial_date::Dates.DateTime
    # number of dimensions columns in file
    number_of_dimensions::Int
    # names of the dimensions
    names_of_dimensions::Vector{String}
    # Inform which dimension represent time
    time_dimension::String
    # unit of the time series
    unit::String
    # Maximum index for each dimension
    maximum_value_of_each_dimension::Vector{Int}
    # Number of time series in the same file
    number_of_time_series::Int
    # Names of the time series
    names_of_time_series::Vector{String}
    # version of the file. This is in case anything changes in the future
    const version::Int
end

function Base.:(==)(a::Metadata, b::Metadata)
    for field in fieldnames(Metadata)
        if getfield(a, field) != getfield(b, field)
            return false
        end
    end
    return true
end

function Metadata(;
    frequency::String = "M",
    initial_date::Dates.DateTime = DateTime(2000),
    names_of_dimensions::Vector{String},
    time_dimension::String,
    unit::String = "",
    maximum_value_of_each_dimension::Vector{Int},
    names_of_time_series::Vector{String},
)
    metadata = Metadata(
        frequency,
        initial_date,
        length(names_of_dimensions),
        names_of_dimensions,
        time_dimension,
        unit,
        maximum_value_of_each_dimension,
        length(names_of_time_series),
        names_of_time_series,
        QUIVER_FILE_VERSION,
    )

    validate_metadata(metadata)

    return metadata
end

function to_toml(metadata::Metadata, filename::String)
    dict_metadata = OrderedDict(
        "frequency" => metadata.frequency,
        "initial_date" => Dates.format(metadata.initial_date, "yyyy-mm-dd HH:MM:SS"),
        "number_of_dimensions" => metadata.number_of_dimensions,
        "names_of_dimensions" => metadata.names_of_dimensions,
        "time_dimension" => metadata.time_dimension,
        "unit" => metadata.unit,
        "maximum_value_of_each_dimension" => metadata.maximum_value_of_each_dimension,
        "number_of_time_series" => metadata.number_of_time_series,
        "names_of_time_series" => metadata.names_of_time_series,
        "version" => metadata.version,
    )
    open(filename, "w") do io
        TOML.print(io, dict_metadata)
    end
    return nothing
end

function from_toml(filename::String)
    dict_metadata = TOML.parsefile(filename)
    metadata = Metadata(
        frequency = dict_metadata["frequency"],
        initial_date = Dates.DateTime(dict_metadata["initial_date"], "yyyy-mm-dd HH:MM:SS"),
        names_of_dimensions = dict_metadata["names_of_dimensions"],
        time_dimension = dict_metadata["time_dimension"],
        unit = dict_metadata["unit"],
        maximum_value_of_each_dimension = dict_metadata["maximum_value_of_each_dimension"],
        names_of_time_series = dict_metadata["names_of_time_series"],
    )
    validate_metadata(metadata)
    return metadata
end

function validate_metadata(metadata::Metadata)

    num_errors = 0

    if metadata.number_of_dimensions != length(metadata.names_of_dimensions)
        @error("The number_of_dimensions ($(metadata.number_of_dimensions)) must be equal to the length of names_of_dimensions ($(metadata.names_of_dimensions)).")
        num_errors += 1
    end

    if metadata.number_of_dimensions != length(metadata.maximum_value_of_each_dimension)
        @error("The number_of_dimensions ($(metadata.number_of_dimensions)) must be equal to the length of maximum_value_of_each_dimension ($(metadata.maximum_value_of_each_dimension)).")
        num_errors += 1
    end

    if metadata.time_dimension ∉ metadata.names_of_dimensions
        @error("The time_dimension ($(metadata.time_dimension)) must be in names_of_dimensions ($(metadata.names_of_dimensions)).")
        num_errors += 1
    end

    if metadata.number_of_time_series != length(metadata.names_of_time_series)
        @error("The number_of_time_series ($(metadata.number_of_time_series)) must be equal to the length of names_of_time_series ($(metadata.names_of_time_series)).")
        num_errors += 1
    end

    if isempty(metadata.names_of_time_series)
        @error("The names_of_time_series must not be empty.")
        num_errors += 1
    end

    if length(unique(metadata.names_of_time_series)) != metadata.number_of_time_series
        @error("The names_of_time_series must be unique.")
        num_errors += 1
    end

    if num_errors > 0
        error("Invalid metadata")
    end

    return nothing
end