
function merge(
    output_filename::String,
    filenames::Vector{String},
    impl::Type{<:Implementation},
)
    readers = [Quiver.Reader{impl}(filename) for filename in filenames]
    metadata = first(readers).metadata
    labels = String[]

    num_dimensions_errors = 0
    num_dimension_size_errors = 0
    num_time_dimension_errors = 0
    num_initial_date_errors = 0
    num_unit_errors = 0
    num_label_errors = 0
    
    for reader in readers
        if metadata.dimensions != reader.metadata.dimensions
            num_dimensions_errors += 1
        end
        if metadata.dimension_size != reader.metadata.dimension_size
            num_dimension_size_errors += 1
        end
        if metadata.time_dimension != reader.metadata.time_dimension
            num_time_dimension_errors += 1
        end
        if metadata.initial_date != reader.metadata.initial_date
            num_initial_date_errors += 1
        end
        if metadata.unit != reader.metadata.unit
            num_unit_errors += 1
        end
        current_label = reader.metadata.labels
        for label in current_label
            if label in labels
                num_label_errors += 1                
            end
        end
        append!(labels, current_label)
    end

    msg = ""
    if num_dimensions_errors > 0
        msg = "$(msg)Dimensions are different.\n"
    end
    if num_dimension_size_errors > 0
        msg = "$(msg)Dimension sizes are different.\n"
    end
    if num_time_dimension_errors > 0
        msg = "$(msg)Time dimensions are different.\n"
    end
    if num_initial_date_errors > 0
        msg = "$(msg)Initial dates are different.\n"
    end
    if num_unit_errors > 0
        msg = "$(msg)Units are different.\n"
    end
    if num_label_errors > 0
        msg = "$(msg)Labels must not repeat.\n"
    end
    if !isempty(msg)
        throw(ArgumentError(msg))
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

    for dims in Iterators.product([1:size for size in reverse(metadata.dimension_size)]...)
        dim_kwargs = OrderedDict(metadata.dimensions .=> reverse(dims))
        data = Float64[]
        for reader in readers
            Quiver.goto!(reader; dim_kwargs...)
            append!(data, reader.data)
        end
        if all(isnan.(data))
            continue
        end
        Quiver.write!(writer, data; dim_kwargs...)
    end

    for reader in readers
        Quiver.close!(reader)
    end

    Quiver.close!(writer)
    return nothing
end
