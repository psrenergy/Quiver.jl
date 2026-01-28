function bin_to_csv(filepath::String; aggregate_time_dimensions::Bool = true)
    reader = Quiver.open_file(filepath, "r")
    metadata = reader.metadata
    csv_writer = initialize_csv_writer(filepath, metadata; aggregate_time_dimensions)

    initial_dim_values = dimension_initial_values(metadata)
    current_dimensions = copy(initial_dim_values)

    for _ in 1:maximum_number_of_lines(metadata)
        data = quiver_read!(reader; Tuple(zip(metadata.dimensions, current_dimensions))...)
        csv_line = build_csv_line(current_dimensions, data, metadata; aggregate_time_dimensions)
        print(csv_writer, csv_line)

        current_sizes = dimension_sizes_at_value(metadata, current_dimensions)
        next_dimensions!(current_dimensions, current_sizes, initial_dim_values, metadata.dimension_parent_indexes)
        if current_dimensions == initial_dim_values
            break
        end
    end

    CSV.close(csv_writer)
    Quiver.close_file(reader)

    return nothing
end

function csv_to_bin(filepath::String)
    metadata = metadata_from_toml(filepath)
    row_iterator, csv_reader, aggregated_time_dimensions_flag = initialize_csv_reader(filepath, metadata)
    (row, state) = iterate(row_iterator)
    writer = Quiver.open_file(filepath, "w", metadata=metadata)

    initial_dim_values = dimension_initial_values(metadata)
    current_dimensions = copy(initial_dim_values)

    for _ in 1:maximum_number_of_lines(metadata)
        validate_csv_dimensions(row, current_dimensions, metadata, aggregated_time_dimensions_flag)

        data = [Float64(row[Symbol(x)]) for x in metadata.labels]
        quiver_write!(writer, data; Tuple(zip(metadata.dimensions, current_dimensions))...)
        
        current_sizes = dimension_sizes_at_value(metadata, current_dimensions)
        next_dimensions!(current_dimensions, current_sizes, initial_dim_values, metadata.dimension_parent_indexes)

        if current_dimensions == initial_dim_values
            break
        end
        (row, state) = iterate(row_iterator, state)
    end

    CSV.close(csv_reader)
    Quiver.close_file(writer)

    return nothing
end

function next_dimensions!(current_dimensions::Vector{Int}, dimension_sizes::Vector{Int}, initial_dim_values::Vector{Int}, dimension_parent_indexes::Vector{Int})
    for i in reverse(1:length(current_dimensions))
        if current_dimensions[i] < dimension_sizes[i]
            current_dimensions[i] += 1
            break
        else
            current_dimensions[i] = 1
        end
    end

    # Correct time dimensions set to 1 incorrectly before the next time dimension is incremented
    # Ex: [month, scenario, day] when initial date is 2025-01-02
    # [1, 1, 31] -> [1, 2, 1] is incorrect, should be [1, 2, 2]
    for i in 1:length(current_dimensions)
        if current_dimensions[i] < initial_dim_values[i] && dimension_parent_indexes[i] != 0 && current_dimensions[dimension_parent_indexes[i]] == initial_dim_values[dimension_parent_indexes[i]]
            current_dimensions[i] = initial_dim_values[i]
        end
    end

    return nothing
end

function initialize_csv_writer(filepath::String, metadata::Metadata; aggregate_time_dimensions::Bool = true)
    header = String[]
    if aggregate_time_dimensions
        if any(isequal(Frequencies.HOURLY), metadata.frequencies)
            push!(header, "datetime")
        else
            push!(header, "date")
        end
    end
    for dim in metadata.dimensions
        if aggregate_time_dimensions && dim in metadata.time_dimensions
            continue
        end
        push!(header, String(dim))
    end
    for label in metadata.labels
        push!(header, label)
    end

    io = open(filepath * ".csv", "w")
    print(io, join(header, ",") * "\n")

    return io
end

function initialize_csv_reader(filepath::String, metadata::Metadata)
    io = open(filepath * ".csv", "r")

    aggregated_time_dimensions_flag = time_dimensions_are_aggregated(io, metadata)

    dim_types = if aggregated_time_dimensions_flag
        [String; fill(Int32, metadata.number_of_dimensions - metadata.number_of_time_dimensions)]
    else
        fill(Int32, metadata.number_of_dimensions)
    end

    csv_reader = CSV.Rows(
        io;
        types = [dim_types; fill(Float32, metadata.number_of_labels)],
        buffer_in_memory = true,
        reusebuffer = true,
    )
    return csv_reader, io, aggregated_time_dimensions_flag
end

function build_csv_line(current_dimensions::Vector{Int}, data::Vector{Float64}, metadata::Metadata; aggregate_time_dimensions::Bool = true)
    line_elements = String[]
    if aggregate_time_dimensions
        date_time = build_datetime_string_from_time_dimensions(current_dimensions, metadata)
        push!(line_elements, date_time)
    end
    for (dim, dim_value) in zip(metadata.dimensions, current_dimensions)
        if aggregate_time_dimensions && dim in metadata.time_dimensions
            continue
        end
        push!(line_elements, string(dim_value))
    end
    for data_value in data
        push!(line_elements, string(round(data_value, digits=6)))
    end

    return join(line_elements, ",") * "\n"
end

function time_dimensions_are_aggregated(io::IO, metadata::Metadata)
    header_line = readline(io)
    second_line = readline(io)
    seekstart(io)
    first_dimension_value = split(second_line, ",")[1]

    aggregated_time_dimensions_flag = false
    
    try
        parse(Int, first_dimension_value)
    catch ArgumentError
        try
            Dates.DateTime(first_dimension_value)
            aggregated_time_dimensions_flag = true
        catch ArgumentError
            error("Error reading first dimension of file. Could not parse $first_dimension_value as Int or DateTime.")
        end
    end

    validate_csv_header(header_line, metadata, aggregated_time_dimensions_flag)

    return aggregated_time_dimensions_flag
end

function expected_dimension_names(metadata::Metadata, aggregated_time_dimensions_flag::Bool)
    dimension_names = String[]
    if aggregated_time_dimensions_flag
        if any(isequal(Frequencies.HOURLY), metadata.frequencies)
            push!(dimension_names, "datetime")
        else
            push!(dimension_names, "date")
        end
        append!(dimension_names, String.(filter(x -> !(x in metadata.time_dimensions), metadata.dimensions)))
    else
        dimension_names = String.(metadata.dimensions)
    end
    return dimension_names
end

function dimension_sizes_at_value(metadata::Metadata, dimension_values::Vector{Int})
    sizes_at_value = copy(metadata.dimension_sizes)
    date_at_value = build_datetime_from_time_dimensions(dimension_values, metadata)

    for (i, dim_freq) in enumerate(metadata.frequencies)
        if i == 1
            continue
        end
        next_dim_freq = metadata.frequencies[i-1]

        # Yearly and weekly frequencies must always be at index 1, so they are not considered in this loop
        if dim_freq == Frequencies.HOURLY
            if next_dim_freq == Frequencies.DAILY
                # Number of hours in a day is always the same
                continue
            elseif next_dim_freq == Frequencies.WEEKLY
                # Number of hours in a week is always the same
                continue
            elseif next_dim_freq == Frequencies.MONTHLY
                sizes_at_value[metadata.time_dimension_indexes[i]] = Dates.daysinmonth(date_at_value) * MAX_HOURS_IN_DAY
            elseif next_dim_freq == Frequencies.YEARLY
                sizes_at_value[metadata.time_dimension_indexes[i]] = Dates.daysinyear(date_at_value) * MAX_HOURS_IN_DAY
            end
        elseif dim_freq == Frequencies.DAILY
            if next_dim_freq == Frequencies.WEEKLY
                # Number of days in a week is always the same
                continue
            elseif next_dim_freq == Frequencies.MONTHLY
                sizes_at_value[metadata.time_dimension_indexes[i]] = Dates.daysinmonth(date_at_value)
            elseif next_dim_freq == Frequencies.YEARLY
                sizes_at_value[metadata.time_dimension_indexes[i]] = Dates.daysinyear(date_at_value)
            end
        elseif dim_freq == Frequencies.MONTHLY
            # Number of months in a year is always the same
            continue
        end
    end

    return sizes_at_value
end