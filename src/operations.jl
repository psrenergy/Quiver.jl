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
        throw(ArgumentError("Apply expression $(func) has $iterator errors.\n\n$msg"))
    end

    labels = union([reader.metadata.labels for reader in readers]...)
    num_labels = length(labels)

    dimensions = metadata.dimensions
    dimension_size = metadata.dimension_size
    reverse_dimensions = (reverse(dimensions))

    writer = Quiver.Writer{impl}(
        output_filename;
        labels = labels,
        dimensions = string.(dimensions),
        time_dimension = string(metadata.time_dimension),
        dimension_size = metadata.dimension_size,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
    )

    index_of_labels_all_readers = [findall(x -> x in reader.metadata.labels, labels) for reader in readers]
    data_all_readers = [zeros(num_labels) for _ in 1:n_readers]
    for dims in Iterators.product([1:size for size in reverse(dimension_size)]...)
        dim_kwargs = OrderedDict(reverse_dimensions .=> dims)
        for (i, reader) in enumerate(readers)
            Quiver.goto!(reader; dim_kwargs...)
            data_all_readers[i][index_of_labels_all_readers[i]] = Float64.(reader.data)
        end
        data = operation.(data_all_readers...)
        Quiver.write!(writer, Quiver.round_digits(data, digits); dim_kwargs...)
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
    reverse_dimensions = (reverse(dimensions))
    n_agents = length(labels)

    if dim_to_operate == metadata.time_dimension
        throw(ArgumentError("Dimension $dim_to_operate is the time dimension. This is not allowed."))
    end
    dim_to_operate_idx = findfirst(x -> x == dim_to_operate, dimensions)
    if dim_to_operate_idx !== nothing
        dim_to_operate_idx = dim_to_operate_idx
        if dim_to_operate_idx != length(dimensions)
            @warn "Dimension $dim_to_operate is not the last dimension. This not the most efficient way to operate over dimensions."
        end
    else
        throw(ArgumentError("Dimension $dim_to_operate not found in file $filename"))
    end

    # Get the sizes of the dimensions
    dimension_size = metadata.dimension_size

    # Define a function to get the indices for all dimensions except the one being operated on
    other_dimension_sizes = [dimension_size[i] for i in eachindex(dimension_size) if i != dim_to_operate_idx]
    other_dimensions = [dimensions[i] for i in eachindex(dimensions) if i != dim_to_operate_idx]
    reverse_dimensions = reverse(dimensions)
    reverse_other_dimensions = reverse(other_dimensions)

    writer = Quiver.Writer{impl}(
        output_filename;
        labels = labels,
        dimensions = string.(other_dimensions),
        time_dimension = string(metadata.time_dimension),
        dimension_size = other_dimension_sizes,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
    )

    # Iterate over all combinations of the other dimensions using column-major order
    for dims in Iterators.product([1:size for size in reverse(other_dimension_sizes)]...)
        dim_kwargs = OrderedDict(reverse_other_dimensions .=> dims)
        dim_kwargs_operate = copy(dim_kwargs)

        data = [zeros(n_agents) for _ in 1:dimension_size[dim_to_operate_idx]]

        # Apply the operation across the specified Dimension
        # TODO: This doesn't respect column major order, but it's not clear how to do that
        for i in 1:dimension_size[dim_to_operate_idx]
            # Set the index for the dimension we are operating on
            dim_kwargs_operate[dimensions[dim_to_operate_idx]] = i
            Quiver.goto!(reader; dim_kwargs_operate...)

            # Store the data at this position
            data[i] = Float64.(reader.data)
        end

        # Apply the operation (e.g., sum) across the dimension
        result = operation.(data...)

        # Write the result to the output file
        Quiver.write!(writer, Quiver.round_digits(result, digits); dim_kwargs...)
    end

    close!(reader)
    close!(writer)
    return nothing
end
