function apply_expression(
    output_filename::String,
    filenames::Vector{String},
    operation::Function,
    impl::Type{<:Implementation};
    digits::Union{Int, Nothing} = nothing,
)
    readers = [Quiver.Reader{impl}(filename) for filename in filenames]
    metadata = first(readers).metadata
    n_readers = length(readers)

    iterator = 0
    msg = ""
    for reader in readers
        if metadata.dimensions != reader.metadata.dimensions
            iterator += 1
            msg = "$(msg)[Error $iterator] Dimensions are different. Dimensions in file $(first(readers).filename) is $(metadata.dimensions) and in file $(reader.filename) is $(reader.metadata.dimensions).\n\n"
        end
        if metadata.dimension_size != reader.metadata.dimension_size
            iterator += 1
            msg = "$(msg)[Error $iterator] Dimension sizes are different. Dimension size in file $(first(readers).filename) is $(metadata.dimension_size) and in file $(reader.filename) is $(reader.metadata.dimension_size).\n\n"
        end
        if metadata.time_dimension != reader.metadata.time_dimension
            iterator += 1
            msg = "$(msg)[Error $iterator] Time dimensions are different. Time dimension in file $(first(readers).filename) is $(metadata.time_dimension) and in file $(reader.filename) is $(reader.metadata.time_dimension).\n\n"
        end
        if metadata.initial_date != reader.metadata.initial_date
            iterator += 1
            msg = "$(msg)[Error $iterator] Initial dates are different. Initial date in file $(first(readers).filename) is $(metadata.initial_date) and in file $(reader.filename) is $(reader.metadata.initial_date).\n\n"
        end
        if metadata.unit != reader.metadata.unit
            iterator += 1
            msg = "$(msg)[Error $iterator] Units are different. Unit in file $(first(readers).filename) is $(metadata.unit) and in file $(reader.filename) is $(reader.metadata.unit).\n\n"
        end
    end

    if !isempty(msg)
        for reader in readers
            Quiver.close!(reader)
        end
        throw(ArgumentError("Apply expression $(operation) has $iterator errors.\n\n$msg"))
    end

    labels = union([reader.metadata.labels for reader in readers]...)
    num_labels = length(labels)

    dimensions = metadata.dimensions
    dimension_size = metadata.dimension_size

    writer = Quiver.Writer{impl}(
        output_filename;
        labels = labels,
        dimensions = string.(dimensions),
        time_dimension = string(metadata.time_dimension),
        dimension_size = metadata.dimension_size,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
        frequency = metadata.frequency,
    )

    dims = Quiver.first_position!(metadata.dimension_size)

    index_of_labels_all_readers = [findall(x -> x in reader.metadata.labels, labels) for reader in readers]
    data_all_readers = [zeros(num_labels) for _ in 1:n_readers]
    for _ in 1:prod(metadata.dimension_size)
        Quiver.next_dim!(dims, metadata.dimension_size)
        for (i, reader) in enumerate(readers)
            Quiver.goto!(reader, dims...)
            data_all_readers[i][index_of_labels_all_readers[i]] = Float64.(reader.data)
        end
        data = operation.(data_all_readers...)
        Quiver.write!(writer, Quiver.round_digits(data, digits), dims...)
    end

    for reader in readers
        Quiver.close!(reader)
    end
    close!(writer)
    return nothing
end

function apply_expression_over_dimension(
    output_filename::String,
    filename::String,
    operation::Function,
    dim_to_operate::Symbol,
    impl::Type{<:Implementation};
    digits::Union{Int, Nothing} = nothing,
)
    reader = Quiver.Reader{impl}(filename)
    metadata = reader.metadata

    labels = metadata.labels
    dimensions = metadata.dimensions
    dimension_size = metadata.dimension_size

    if dim_to_operate == metadata.time_dimension
        Quiver.close!(reader)
        throw(ArgumentError("Dimension $dim_to_operate is the time dimension. This is not allowed."))
    end
    dim_to_operate_idx = findfirst(x -> x == dim_to_operate, dimensions)
    if dim_to_operate_idx !== nothing
        dim_to_operate_idx = dim_to_operate_idx
        if dim_to_operate_idx != length(dimensions)
            if impl == Quiver.CSV
                throw(ArgumentError("Dimension $dim_to_operate is not the last dimension. This is not allowed for CSV files."))
            end
            @warn "Dimension $dim_to_operate is not the last dimension. This is not the most efficient way to operate over dimensions."
        end
    else
        Quiver.close!(reader)
        throw(ArgumentError("Dimension $dim_to_operate not found in file $filename"))
    end

    all_idxs = 1:length(dimensions)
    other_dims_idx = filter(i -> i != dim_to_operate_idx, all_idxs)
    other_dimension_sizes = dimension_size[other_dims_idx]

    writer = Quiver.Writer{impl}(
        output_filename;
        labels = labels,
        dimensions = string.(dimensions[other_dims_idx]),
        time_dimension = string(metadata.time_dimension),
        dimension_size = other_dimension_sizes,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
        frequency = metadata.frequency,
    )

    dims = Quiver.first_position!(other_dimension_sizes)
    for _ in 1:prod(other_dimension_sizes)
        Quiver.next_dim!(dims, other_dimension_sizes)
        dims_operate = Vector{Int}(undef, length(dimensions))
        dims_operate[other_dims_idx] .= Tuple(dims)

        data = zeros(length(labels), dimension_size[dim_to_operate_idx])
        for i in 1:dimension_size[dim_to_operate_idx]
            dims_operate[dim_to_operate_idx] = i
            Quiver.goto!(reader, dims_operate...)
            data[:, i] = Float64.(reader.data)
        end

        result = operation(data, dims = 2)[:, 1]
        Quiver.write!(writer, Quiver.round_digits(result, digits), dims...)
    end

    close!(reader)
    close!(writer)
    return nothing
end

function apply_expression_over_agents(
    output_filename::String,
    filename::String,
    operation::Function,
    new_labels::Vector{String},
    impl::Type{<:Implementation};
    digits::Union{Int, Nothing} = nothing,
)
    reader = Quiver.Reader{impl}(filename)
    metadata = reader.metadata

    labels = metadata.labels
    dimensions = metadata.dimensions
    dimension_size = metadata.dimension_size
    n_agents = length(labels)
    n_new_agents = length(new_labels)

    data_test = ones(n_agents)
    result_test = vcat(operation(data_test))
    if length(result_test) != n_new_agents
        Quiver.close!(reader)
        throw(ArgumentError("The number of agents in the result of the operation is different from the number of agents in the output file."))
    end

    writer = Quiver.Writer{impl}(
        output_filename;
        labels = new_labels,
        dimensions = string.(dimensions),
        time_dimension = string(metadata.time_dimension),
        dimension_size = dimension_size,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
        frequency = metadata.frequency,
    )

    dims = Quiver.first_position!(dimension_size)

    data = zeros(n_new_agents)
    # Iterate over all combinations of the other dimensions using column-major order
    for _ in 1:prod(dimension_size)
        Quiver.next_dim!(dims, dimension_size)
        Quiver.goto!(reader, dims...)
        data = vcat(operation(reader.data))
        # Write the result to the output file
        Quiver.write!(writer, Quiver.round_digits(data, digits), dims...)
    end

    close!(reader)
    close!(writer)
    return nothing
end
