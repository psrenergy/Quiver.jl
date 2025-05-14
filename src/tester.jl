function Tester{impl}(
    filename::String; 
    expected_frequency::Union{String, Nothing} = nothing,
    expected_initial_date::Union{Dates.DateTime, Nothing} = nothing,
    expected_number_of_dimensions::Union{Int, Nothing} = nothing,
    expected_dimensions::Union{Vector{String}, Nothing} = nothing,
    expected_time_dimension::Union{String, Nothing} = nothing,
    expected_unit::Union{String, Nothing} = nothing,
    expected_dimension_size::Union{Vector{Int}, Nothing} = nothing,
    expected_number_of_time_series::Union{Int, Nothing} = nothing,
    expected_labels::Union{Vector{String}, Nothing} = nothing,
)
    n_errors = 0

    reader = Quiver.Reader{impl}(filename)

    if !isnothing(expected_frequency) && expected_frequency != reader.metadata.frequency
        n_errors += 1
        println("Error: Expected frequency $(expected_frequency), but got $(reader.metadata.frequency).")
    end

    if !isnothing(expected_initial_date) && expected_initial_date != reader.metadata.initial_date
        n_errors += 1
        println("Error: Expected initial date $(expected_initial_date), but got $(reader.metadata.initial_date).")
    end

    if !isnothing(expected_number_of_dimensions) && expected_number_of_dimensions != reader.metadata.number_of_dimensions
        n_errors += 1
        println("Error: Expected number of dimensions $(expected_number_of_dimensions), but got $(reader.metadata.number_of_dimensions).")
    end

    if !isnothing(expected_dimensions) && expected_dimensions != reader.metadata.dimensions
        n_errors += 1
        println("Error: Expected dimensions $(expected_dimensions), but got $(reader.metadata.dimensions).")
    end

    if !isnothing(expected_time_dimension) && expected_time_dimension != reader.metadata.time_dimension
        n_errors += 1
        println("Error: Expected time dimension $(expected_time_dimension), but got $(reader.metadata.time_dimension).")
    end

    if !isnothing(expected_unit) && expected_unit != reader.metadata.unit
        n_errors += 1
        println("Error: Expected unit $(expected_unit), but got $(reader.metadata.unit).")
    end

    if !isnothing(expected_dimension_size) && expected_dimension_size != reader.metadata.dimension_size
        n_errors += 1
        println("Error: Expected dimension size $(expected_dimension_size), but got $(reader.metadata.dimension_size).")
    end

    if !isnothing(expected_number_of_time_series) && expected_number_of_time_series != reader.metadata.number_of_time_series
        n_errors += 1
        println("Error: Expected number of time series $(expected_number_of_time_series), but got $(reader.metadata.number_of_time_series).")
    end

    if !isnothing(expected_labels) && expected_labels != reader.metadata.labels
        n_errors += 1
        println("Error: Expected labels $(expected_labels), but got $(reader.metadata.labels).")
    end

    close(reader)

    return n_errors
end