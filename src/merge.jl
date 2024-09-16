function merge(
    output_filename::String,
    filenames::Vector{String},
    impl::Type{<:Implementation};
    digits::Union{Int, Nothing} = nothing,
)
    readers = [Quiver.Reader{impl}(filename) for filename in filenames]
    metadata = first(readers).metadata
    labels = String[]

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
        current_label = reader.metadata.labels
        for label in current_label
            if label in labels
                iterator += 1
                msg = "$(msg)[Error $iterator] Label $(label) in file $(first(readers).filename) is already in the merged labels.\n\n"
            end
        end
        append!(labels, current_label)
    end

    if !isempty(msg)
        throw(ArgumentError("Merge has $iterator errors.\n\n$msg"))
    end
    
    writer = Quiver.Writer{impl}(
        output_filename;
        labels = labels,
        dimensions = string.(metadata.dimensions),
        time_dimension = string(metadata.time_dimension),
        dimension_size = metadata.dimension_size,
        initial_date = metadata.initial_date,
        unit = metadata.unit,
    )

    num_labels = [length(reader.metadata.labels) for reader in readers]
    data = zeros(sum(num_labels))
    for dims in Iterators.product([1:size for size in reverse(metadata.dimension_size)]...)
        dim_kwargs = OrderedDict(metadata.dimensions .=> reverse(dims))
        for (i, reader) in enumerate(readers)
            Quiver.goto!(reader; dim_kwargs...)
            if i == 1
                initial_idx = 1
            else
                initial_idx = sum(num_labels[1:i-1]) + 1
            end
            final_idx = sum(num_labels[1:i])
            data[initial_idx:final_idx] = reader.data
        end
        if all(isnan.(data))
            continue
        end
        Quiver.write!(writer, round_digits(data, digits); dim_kwargs...)
    end

    for reader in readers
        Quiver.close!(reader)
    end

    Quiver.close!(writer)
    return nothing
end
