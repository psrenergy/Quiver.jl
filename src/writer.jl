mutable struct Writer{I <: Implementation, W}
    writer::W
    filename::String
    metadata::Metadata
    last_dimension_added::Vector{Int}
    function Writer{I}(
        writer::W, 
        filename::String, 
        metadata::Metadata, 
        last_dimension_added::Vector{Int}
    ) where {I, W}
        writer = new{I, W}(writer, filename, metadata, last_dimension_added)
        finalizer(Quiver.close!, writer)
        return writer
    end
end

function _build_last_dimension_added!(writer::Writer; dims...)
    for (i, dim) in enumerate(writer.metadata.dimensions)
        writer.last_dimension_added[i] = dims[dim]
    end
    return nothing
end

function write!(writer::Writer, data::Vector{T}; dims...) where T <: Real
    validate_dimensions(writer.metadata, dims...)
    _build_last_dimension_added!(writer; dims...)
    _quiver_write!(writer, data)
end

function close!(writer::Writer)
    _quiver_close!(writer)
    return nothing
end

"""
    array_to_file(
        filename::String,
        data::Array{T, N},
        implementation::Type{I};
        dimensions::Vector{String},
        labels::Vector{String},
        time_dimension::String,
        dimension_size::Vector{Int},
        initial_date::Union{String, DateTime} = "",
        unit::String = "",
        digits::Union{Int, Nothing} = nothing,
    ) where {I <:Implementation, T, N}

Write a time series file in Quiver format.

Required arguments:

  - `file_path::String`: Path to file.
  - `data::Array{T, N}`: Data to be written.
  - `implementation::Type{I}`: Implementation to be used. It can be `Quiver.csv` or `Quiver.binary`.
  - `dimensions::Vector{String}`: Dimensions of the data.
  - `labels::Vector{String}`: Labels of the data.
  - `time_dimension::String`: Name of the time dimension.
  - `dimension_size::Vector{Int}`: Size of each dimension.
  - `initial_date::Union{String, DateTime}`: Initial date of the time series. If a string is provided, it should be in the format "yyyy-mm-ddTHH:MM:SS".

Optional arguments:
  - `digits::Union{Int, Nothing}`: Number of digits to round the data. If nothing is provided, the data is not rounded.
  - `unit::String`: Unit of the time series data.
"""
function array_to_file(
    filename::String,
    data::Array{T, N},
    implementation::Type{I};
    dimensions::Vector{String},
    labels::Vector{String},
    time_dimension::String,
    dimension_size::Vector{Int},
    initial_date::Union{String, DateTime} = "",
    unit::String = "",
    digits::Union{Int, Nothing} = nothing,
) where {I <: Implementation, T, N}
    kwargs_dict = Dict{Symbol, Any}()
    if initial_date !== ""
        if isa(initial_date, String)
            initial_date = DateTime(initial_date, "yyyy-mm-ddTHH:MM:SS")
        end
        kwargs_dict[:initial_date] = initial_date
    end
    if unit != ""
        kwargs_dict[:unit] = unit
    else
        @warn("No unit was provided for the time series file \"$filename\".")
    end

    writer = Quiver.Writer{implementation}(
        filename;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        kwargs_dict...,
    )

    reverse_dimensions = Symbol.(reverse(dimensions))

    for dims in Iterators.product([1:size for size in reverse(dimension_size)]...)
        dim_kwargs = OrderedDict(reverse_dimensions .=> dims)
        Quiver.write!(writer, round_digits(data[:, dims...], digits); dim_kwargs...)
    end

    Quiver.close!(writer)

    return nothing
end

function round_digits(vec::Vector{T}, ::Nothing) where {T}
    return vec
end

function round_digits(vec::Vector{T}, digits::Int) where {T}
    return round.(vec; digits)
end