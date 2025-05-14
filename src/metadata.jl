mutable struct Metadata
    # frequency of the time series
    frequency::String
    # initial date of the time series
    initial_date::Dates.DateTime
    # number of dimensions columns in file
    number_of_dimensions::Int
    # names of the dimensions
    dimensions::Vector{Symbol}
    # Inform which dimension represent time
    time_dimension::Symbol
    # unit of the time series
    unit::String
    # Maximum index for each dimension
    dimension_size::Vector{Int}
    # Number of time series in the same file
    number_of_time_series::Int
    # Names of the time series
    labels::Vector{String}
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
    frequency::String = "month",
    initial_date::Dates.DateTime = DateTime(2000),
    dimensions::Vector{String},
    time_dimension::String,
    unit::String = "",
    dimension_size::Vector{Int},
    labels::Vector{String},
)
    metadata = Metadata(
        frequency,
        initial_date,
        length(dimensions),
        Symbol.(dimensions),
        Symbol(time_dimension),
        unit,
        dimension_size,
        length(labels),
        labels,
        QUIVER_FILE_VERSION,
    )

    validate_metadata(metadata)

    return metadata
end

function to_ordered_dict(metadata::Metadata)
    return OrderedDict(
        "version" => metadata.version,
        "dimensions" => String.(metadata.dimensions),
        "dimension_size" => metadata.dimension_size,
        "initial_date" => Dates.format(metadata.initial_date, "yyyy-mm-dd HH:MM:SS"),
        "time_dimension" => String(metadata.time_dimension),
        "frequency" => metadata.frequency,
        "unit" => metadata.unit,
        "labels" => metadata.labels,
    )
end

function to_toml(metadata::Metadata, filename::String)
    dict_metadata = to_ordered_dict(metadata)
    open(filename, "w") do io
        TOML.print(io, dict_metadata)
        return nothing
    end
    return nothing
end

function from_toml(filename::String)
    dict_metadata = TOML.parsefile(filename)
    metadata = Metadata(
        frequency = dict_metadata["frequency"],
        initial_date = Dates.DateTime(dict_metadata["initial_date"], "yyyy-mm-dd HH:MM:SS"),
        dimensions = dict_metadata["dimensions"],
        time_dimension = dict_metadata["time_dimension"],
        unit = dict_metadata["unit"],
        dimension_size = dict_metadata["dimension_size"],
        labels = dict_metadata["labels"],
    )
    validate_metadata(metadata)
    return metadata
end

function validate_metadata(metadata::Metadata)
    num_errors = 0

    if metadata.number_of_dimensions != length(metadata.dimensions)
        @error("The number_of_dimensions ($(metadata.number_of_dimensions)) must be equal to the length of dimensions ($(metadata.dimensions)).")
        num_errors += 1
    end

    if metadata.number_of_dimensions != length(metadata.dimension_size)
        @error(
            "The number_of_dimensions ($(metadata.number_of_dimensions)) must be equal to the length of dimension_size ($(metadata.dimension_size))."
        )
        num_errors += 1
    end

    if metadata.time_dimension ∉ metadata.dimensions
        @error("The time_dimension ($(metadata.time_dimension)) must be in dimensions ($(metadata.dimensions)).")
        num_errors += 1
    end

    if metadata.number_of_time_series != length(metadata.labels)
        @error("The number_of_time_series ($(metadata.number_of_time_series)) must be equal to the length of labels ($(metadata.labels)).")
        num_errors += 1
    end

    if isempty(metadata.labels)
        @error("The labels must not be empty.")
        num_errors += 1
    end

    if length(unique(metadata.labels)) != metadata.number_of_time_series
        @error("The labels must be unique.")
        num_errors += 1
    end

    if num_errors > 0
        error("Invalid metadata")
    end

    return nothing
end

function test(
    metadata::Metadata;
    expected_frequency::Union{String, Nothing} = nothing,
    expected_initial_date::Union{Dates.DateTime, Nothing} = nothing,
    expected_number_of_dimensions::Union{Int, Nothing} = nothing,
    expected_dimensions::Union{Vector{String}, Nothing} = nothing,
    expected_time_dimension::Union{String, Nothing} = nothing,
    expected_unit::Union{String, Nothing} = nothing,
    expected_dimension_size::Union{Vector{Int}, Nothing} = nothing,
    expected_number_of_time_series::Union{Int, Nothing} = nothing,
    expected_labels::Union{Vector{String}, Nothing} = nothing,
)::Bool
    if !isnothing(expected_frequency) && expected_frequency != metadata.frequency
        @error("Expected frequency $(expected_frequency), but got $(metadata.frequency).")
        return false
    end

    if !isnothing(expected_initial_date) && expected_initial_date != metadata.initial_date
        @error("Expected initial date $(expected_initial_date), but got $(metadata.initial_date).")
        return false
    end

    if !isnothing(expected_number_of_dimensions) && expected_number_of_dimensions != metadata.number_of_dimensions
        @error("Expected number of dimensions $(expected_number_of_dimensions), but got $(metadata.number_of_dimensions).")
        return false
    end

    if !isnothing(expected_dimensions) && expected_dimensions != string.(metadata.dimensions)
        @error("Expected dimensions $(expected_dimensions), but got $(string.(metadata.dimensions)).")
        return false
    end

    if !isnothing(expected_time_dimension) && expected_time_dimension != string(metadata.time_dimension)
        @error("Expected time dimension $(expected_time_dimension), but got $(string(metadata.time_dimension)).")
        return false
    end

    if !isnothing(expected_unit) && expected_unit != metadata.unit
        @error("Expected unit $(expected_unit), but got $(metadata.unit).")
        return false
    end

    if !isnothing(expected_dimension_size) && expected_dimension_size != metadata.dimension_size
        @error("Expected dimension size $(expected_dimension_size), but got $(metadata.dimension_size).")
        return false
    end

    if !isnothing(expected_number_of_time_series) && expected_number_of_time_series != metadata.number_of_time_series
        @error("Expected number of time series $(expected_number_of_time_series), but got $(metadata.number_of_time_series).")
        return false
    end

    if !isnothing(expected_labels) && expected_labels != metadata.labels
        @error("Expected labels $(expected_labels), but got $(metadata.labels).")
        return false
    end

    return true
end