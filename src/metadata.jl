struct Metadata <: AbstractMetadata
    # Explicit metadata
    dimensions::Vector{Symbol}
    dimension_sizes::Vector{Int}
    time_dimensions::Vector{Symbol}
    frequencies::Vector{Frequencies.T}
    initial_date::Dates.DateTime
    unit::String
    labels::Vector{String}
    version::Int
    
    # Derived metadata
    number_of_dimensions::Int
    number_of_time_dimensions::Int
    number_of_labels::Int
    time_dimension_initial_values::Vector{Int}
    time_dimension_indexes::Vector{Int}
    dimension_parent_indexes::Vector{Int}
end

function Metadata(;
    dimensions::Vector{String},
    dimension_sizes::Vector{Int},
    time_dimensions::Vector{String},
    frequencies::Vector{String},
    initial_date::String,
    unit::String,
    labels::Vector{String},
    version::Int = QUIVER_FILE_VERSION,
)
    # Transform inputs
    dimensions = Symbol.(dimensions)
    time_dimensions = Symbol.(time_dimensions)
    frequencies = frequency_string_to_enum.(frequencies)
    initial_date = Dates.DateTime(initial_date)

    # Derived metadata
    number_of_dimensions = length(dimensions)
    number_of_time_dimensions = length(time_dimensions)
    number_of_labels = length(labels)
    time_dimension_initial_values = compute_time_dimension_initial_values(initial_date, frequencies)
    time_dimension_indexes = findall(dim -> dim in time_dimensions, dimensions)
    dimension_parent_indexes = build_dimension_parent_indexes(number_of_dimensions, time_dimension_indexes)

    metadata = Metadata(
        # Explicit metadata
        dimensions,
        dimension_sizes,
        time_dimensions,
        frequencies,
        initial_date,
        unit,
        labels,
        version,
        # Derived metadata
        number_of_dimensions,
        number_of_time_dimensions,
        number_of_labels,
        time_dimension_initial_values,
        time_dimension_indexes,
        dimension_parent_indexes,
    )

    validate_metadata(metadata)

    return metadata
end

function metadata_from_toml(filepath::String)
    metadata_filepath = filepath * ".toml"
    metadata_dict = Dict()

    for (key, value) in TOML.parsefile(metadata_filepath)
        metadata_dict[Symbol(key)] = value
    end

    metadata = Metadata(;
        metadata_dict...
    )
    validate_metadata(metadata)

    return metadata
end

function metadata_to_toml(metadata::Metadata, filepath::String)
    validate_metadata(metadata)

    metadata_dict = Dict(
        "version" => QUIVER_FILE_VERSION,
        "dimensions" => String.(metadata.dimensions),
        "dimension_sizes" => metadata.dimension_sizes,
        "time_dimensions" => String.(metadata.time_dimensions),
        "frequencies" => frequency_enum_to_string.(metadata.frequencies),
        "initial_date" => Dates.format(metadata.initial_date, "yyyy-mm-ddTHH:MM:SS"),
        "unit" => metadata.unit,
        "labels" => metadata.labels,
    )

    metadata_filepath = filepath * ".toml"
    open(metadata_filepath, "w") do io
        TOML.print(io, metadata_dict)
    end

    return nothing
end

function time_dimension_sizes(metadata::Metadata)
    time_dim_sizes = metadata.dimension_sizes[metadata.time_dimension_indexes]
    return time_dim_sizes
end

function dimension_initial_values(metadata::Metadata)
    dim_initial_values = ones(Int, metadata.number_of_dimensions)
    for (idx, initial_value) in enumerate(metadata.time_dimension_initial_values)
        time_dim_idx = metadata.time_dimension_indexes[idx]
        dim_initial_values[time_dim_idx] = initial_value
    end
    return dim_initial_values
end

function build_dimension_parent_indexes(number_of_dimensions::Int, time_dimension_indexes::Vector{Int})
    dimension_parent_indexes = zeros(Int, number_of_dimensions)
    for (i, time_dim_idx) in enumerate(time_dimension_indexes)
        if i == 1
            continue
        end
        dimension_parent_indexes[time_dim_idx] = time_dimension_indexes[i-1]
    end

    return dimension_parent_indexes
end

function maximum_number_of_lines(metadata::Metadata)
    max_lines = prod(metadata.dimension_sizes)
    # Remove lines before initial time dimension values
    for (idx, time_dim_idx) in enumerate(metadata.time_dimension_indexes)
        # Find all dimensions before the current time dimension that are not time dimensions
        dimensions_with_missing_lines = setdiff(1:time_dim_idx, metadata.time_dimension_indexes)
        if isempty(dimensions_with_missing_lines)
            continue
        end
        missing_lines = (metadata.time_dimension_initial_values[idx] - 1) * prod(metadata.dimension_sizes[dimensions_with_missing_lines])
        max_lines -= missing_lines
    end
    return max_lines
end

function compute_time_dimension_initial_values(initial_date::Dates.DateTime, frequencies::Vector{Frequencies.T})
    initial_values = ones(Int, length(frequencies))

    for (i, freq) in enumerate(frequencies)
        # The largest time dimension always starts at 1
        if i == 1
            continue
        end
        next_dim_freq = frequencies[i-1]

        # Yearly and weekly frequencies must always be at index 1, so they are not considered in this loop
        if freq == Frequencies.HOURLY
            if next_dim_freq == Frequencies.DAILY
                initial_values[i] = Dates.hour(initial_date)
            elseif next_dim_freq == Frequencies.WEEKLY
                initial_values[i] = Dates.hour(initial_date) + (day_of_week_from_datetime(initial_date) - 1) * MAX_HOURS_IN_DAY
            elseif next_dim_freq == Frequencies.MONTHLY
                initial_values[i] = Dates.hour(initial_date) + (Dates.day(initial_date) - 1) * MAX_HOURS_IN_DAY
            elseif next_dim_freq == Frequencies.YEARLY
                initial_values[i] = Dates.hour(initial_date) + (Dates.dayofyear(initial_date) - 1) * MAX_HOURS_IN_DAY
            end
            initial_values[i] += 1  # Convert from 0-based to 1-based
        elseif freq == Frequencies.DAILY
            if next_dim_freq == Frequencies.WEEKLY
                initial_values[i] = day_of_week_from_datetime(initial_date)
            elseif next_dim_freq == Frequencies.MONTHLY
                initial_values[i] = Dates.day(initial_date)
            elseif next_dim_freq == Frequencies.YEARLY
                initial_values[i] = Dates.dayofyear(initial_date)
            end
        elseif freq == Frequencies.MONTHLY
            initial_values[i] = Dates.month(initial_date)
        else
            error("Unsupported frequency: $freq")
        end
    end

    return initial_values
end
