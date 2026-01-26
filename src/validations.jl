function validate_dims(metadata::Metadata; dims...)
    # Check count
    if length(dims) != metadata.number_of_dimensions
        error("Expected $(metadata.number_of_dimensions) dimensions, got $(length(dims))")
    end

    # Check all dimension names exist and values are in bounds
    for (i, dim_name) in enumerate(metadata.dimensions)
        if !haskey(dims, dim_name)
            error("Missing required dimension: '$dim_name'")
        end

        dim_value = dims[dim_name]

        if dim_value < 1 || dim_value > metadata.dimension_sizes[i]
            error("Dimension '$dim_name' value $dim_value is out of bounds [1, $(metadata.dimension_sizes[i])]")
        end
    end

    validate_time_dimension_values(metadata; dims...)

    return nothing
end

function validate_data_length(data::Vector{Float64}, metadata::Metadata)
    if length(data) != metadata.number_of_labels
        error("Data length $(length(data)) does not match expected length $(metadata.number_of_labels)")
    end

    return nothing
end

function validate_file_exists(filepath::String)
    if !isfile(filepath * ".qvr") || !isfile(filepath * ".toml")
        error("File does not exist: $filepath")
    end
    return nothing
end

function validate_metadata(metadata::Metadata)
    # Version check
    if metadata.version != QUIVER_FILE_VERSION
        error("Incompatible file version: expected $QUIVER_FILE_VERSION, got $(metadata.version)")
    end

    # Dimension consistency
    if length(metadata.dimensions) != metadata.number_of_dimensions
        error("Dimension count mismatch: dimensions has $(length(metadata.dimensions)) elements, but number_of_dimensions is $(metadata.number_of_dimensions)")
    end

    if length(metadata.dimension_sizes) != metadata.number_of_dimensions
        error("Dimension sizes count mismatch: dimension_sizes has $(length(metadata.dimension_sizes)) elements, but number_of_dimensions is $(metadata.number_of_dimensions)")
    end

    # Label consistency
    if length(metadata.labels) != metadata.number_of_labels
        error("Label count mismatch: labels has $(length(metadata.labels)) elements, but number_of_labels is $(metadata.number_of_labels)")
    end

    if metadata.number_of_labels <= 0
        error("Number of labels must be positive, got $(metadata.number_of_labels)")
    end

    # Time dimension consistency
    if length(metadata.time_dimensions) != metadata.number_of_time_dimensions
        error("Time dimension count mismatch: time_dimensions has $(length(metadata.time_dimensions)) elements, but number_of_time_dimensions is $(metadata.number_of_time_dimensions)")
    end

    if length(metadata.frequencies) != metadata.number_of_time_dimensions
        error("Frequencies count mismatch: expected $(metadata.number_of_time_dimensions) frequencies, got $(length(metadata.frequencies))")
    end

    if length(metadata.time_dimension_initial_values) != metadata.number_of_time_dimensions
        error("Initial date values count mismatch: expected $(metadata.number_of_time_dimensions) values, got $(length(metadata.time_dimension_initial_values))")
    end

    # Value constraints
    for (i, size) in enumerate(metadata.dimension_sizes)
        if size <= 0
            error("Dimension size at index $i must be positive, got $size")
        end
    end

    # Uniqueness checks
    if length(unique(metadata.dimensions)) != length(metadata.dimensions)
        error("Dimension names must be unique, found duplicates in: $(metadata.dimensions)")
    end

    if length(unique(metadata.labels)) != length(metadata.labels)
        error("Label names must be unique, found duplicates in: $(metadata.labels)")
    end

    # Subset check: time_dimensions must be subset of dimensions
    for time_dim in metadata.time_dimensions
        if !(time_dim in metadata.dimensions)
            error("Time dimension '$time_dim' is not in dimensions list: $(metadata.dimensions)")
        end
    end

    validate_time_dimension_metadata(metadata)

    return nothing
end

function validate_file_mode(mode::String)
    if mode != "r" && mode != "w"
        error("Unsupported file mode: $mode. Expected 'r' or 'w'.")
    end
    return nothing
end

function validate_file_is_open(file_pointer::FilePointer)
    if !isopen(file_pointer.io)
        error("File is not open: $(file_pointer.filepath)")
    end
    return nothing
end

function validate_not_negative(value::Int, name::String)
    if value < 0
        error("$name must be non-negative, got $value")
    end
    return nothing
end

function validate_csv_header(header_line::String, metadata::Metadata, aggregated_time_dimensions_flag::Bool)
    dimension_names = expected_dimension_names(metadata, aggregated_time_dimensions_flag)
    expected_header = [dimension_names; metadata.labels]
    for (i, header_element) in enumerate(split(header_line, ","))
        if header_element != expected_header[i]
            error("Unexpected header in CSV file: '$header_line'. Expected columns are: $(expected_header)")
        end
    end
    return nothing
end

function validate_csv_dimensions(row::CSV.Row2, current_dimensions::Vector{Int}, metadata::Metadata, aggregated_time_dimensions_flag::Bool)
    dimension_names = expected_dimension_names(metadata, aggregated_time_dimensions_flag)
    expected_dimension_values = copy(current_dimensions)

    if aggregated_time_dimensions_flag
        date_time = build_datetime_string_from_time_dimensions(current_dimensions, metadata)
        indexes_for_other_dimensions = Int[]
        for (i, dim) in enumerate(metadata.dimensions)
            if dim in metadata.time_dimensions
                continue
            end
            push!(indexes_for_other_dimensions, i)
        end
        expected_dimension_values = [date_time; expected_dimension_values[indexes_for_other_dimensions]]
    end

    for (i, name) in enumerate(dimension_names)
        dim_value = row[Symbol(name)]
        if dim_value != expected_dimension_values[i]
            error("CSV dimension '$name' has value $dim_value, expected $(expected_dimension_values[i])")
        end
    end

    return nothing
end

function validate_time_dimension_metadata(metadata::Metadata)

    if !allunique(metadata.frequencies)
        error("Time dimension frequencies must be unique. Got: $(metadata.frequencies)")
    end

    if Frequencies.MONTHLY in metadata.frequencies && Frequencies.WEEKLY in metadata.frequencies
        error("Time dimension frequencies cannot contain both MONTHLY and WEEKLY frequencies.")
    end

    if !issorted(metadata.frequencies)
        error("Time dimension frequencies must be ordered from lowest to highest frequency. Got: $(metadata.frequencies)")
    end

    # TODO: remove the reverse here, it is not necessary and confuses the code
    for (idx, (dim_name, dim_freq, dim_size)) in enumerate(Iterators.reverse(zip(metadata.time_dimensions, metadata.frequencies, time_dimension_sizes(metadata))))
        if idx < metadata.number_of_time_dimensions
            next_dim_freq = metadata.frequencies[metadata.number_of_time_dimensions - idx]
            validate_time_dimension_size(dim_name, dim_freq, dim_size, next_dim_freq)
        end
    end

    return nothing
end

function validate_time_dimension_size(dim_name::Symbol, dim_freq::Frequencies.T, dim_size::Int, next_dim_freq::Frequencies.T)
    min_size = Inf
    max_size = -Inf

    if dim_freq == Frequencies.HOURLY
        if next_dim_freq == Frequencies.DAILY
            min_size = MIN_HOURS_IN_DAY
            max_size = MAX_HOURS_IN_DAY
        elseif next_dim_freq == Frequencies.WEEKLY
            min_size = MIN_HOURS_IN_WEEK
            max_size = MAX_HOURS_IN_WEEK
        elseif next_dim_freq == Frequencies.MONTHLY
            min_size = MIN_HOURS_IN_MONTH
            max_size = MAX_HOURS_IN_MONTH
        elseif next_dim_freq == Frequencies.YEARLY
            min_size = MIN_HOURS_IN_YEAR
            max_size = MAX_HOURS_IN_YEAR
        end
    elseif dim_freq == Frequencies.DAILY
        if next_dim_freq == Frequencies.WEEKLY
            min_size = MIN_DAYS_IN_WEEK
            max_size = MAX_DAYS_IN_WEEK
        elseif next_dim_freq == Frequencies.MONTHLY
            min_size = MIN_DAYS_IN_MONTH
            max_size = MAX_DAYS_IN_MONTH
        elseif next_dim_freq == Frequencies.YEARLY
            min_size = MIN_DAYS_IN_YEAR
            max_size = MAX_DAYS_IN_YEAR
        end
    elseif dim_freq == Frequencies.WEEKLY
        if next_dim_freq == Frequencies.YEARLY
            min_size = MIN_WEEKS_IN_YEAR
            max_size = MAX_WEEKS_IN_YEAR
        end
    elseif dim_freq == Frequencies.MONTHLY
        if next_dim_freq == Frequencies.YEARLY
            min_size = MIN_MONTHS_IN_YEAR
            max_size = MAX_MONTHS_IN_YEAR
        end
    end

    if dim_size < min_size || dim_size > max_size
        error("Time dimension \"$dim_name\" with frequency \"$dim_freq\" has size $dim_size which is out of bounds [$min_size, $max_size] based on the next lower frequency: \"$next_dim_freq\".")
    end

    return nothing
end

function validate_time_dimension_values(metadata::Metadata; dims...)
    dimension_values = collect(dims[metadata.dimensions])
    date_at_dims = build_datetime_from_time_dimensions(dimension_values, metadata)

    for (idx, time_dim_idx) in enumerate(metadata.time_dimension_indexes)
        if idx == 1
            continue
        end
        expected_value = dimension_values[time_dim_idx]
        freq = metadata.frequencies[idx]
        resulting_value = extract_time_dimension_value_from_datetime(date_at_dims, freq)

        if expected_value != resulting_value
            error("Invalid values for time dimensions. Values $(dimension_values[metadata.time_dimension_indexes]) for dimensions $(metadata.time_dimensions) would produce the date: $date_at_dims.")
        end
    end

    return nothing
end
