"""
    Reader{I <: Implementation, R}(reader::R, filename::String, metadata::Metadata,
                                   dimension_in_cache::Vector{Int}, dimension_to_read::Vector{Int};
                                   labels_to_read::Vector{String} = metadata.labels,
                                   carrousel::Bool = false)

Creates a new instance of `Reader`, which is used to manage time series files.
The `Reader` manages the dimensions and labels of the file and allows data access via functions like `goto!` and `next_dimension!`.

# Parameters

  - `reader::R`: The reader responsible for reading the file (can be for binary or CSV formats).
  - `filename::String`: The name of the file containing the time series data.
  - `metadata::Metadata`: Metadata describing the file’s dimensions and labels.
  - `dimension_in_cache::Vector{Int}`: Vector of dimensions kept in cache.
  - `dimension_to_read::Vector{Int}`: Specific dimensions to be read.
  - `labels_to_read::Vector{String}`: Labels to be read from the file (optional).
  - `carrousel::Bool`: Defines whether the reader uses a carousel to access dimensions (optional).

# Returns

  - A new `Reader` object.
"""
mutable struct Reader{I <: Implementation, R}
    reader::R
    filename::String
    metadata::Metadata
    dimension_in_cache::Vector{Int}
    dimension_to_read::Vector{Int}
    all_labels_data_cache::Vector{Float32}
    data::Vector{Float32}
    labels_to_read::Vector{String}
    indices_of_labels_to_read::Vector{Int}
    carrousel::Bool
    function Reader{I}(
        reader::R,
        filename::String,
        metadata::Metadata,
        dimension_in_cache::Vector{Int},
        dimension_to_read::Vector{Int};
        labels_to_read::Vector{String} = metadata.labels,
        carrousel::Bool = false,
    ) where {I, R}

        # Argument validations
        if length(labels_to_read) == 0
            throw(ArgumentError("labels_to_read cannot be empty"))
        end

        # Find the indices of the labels to read
        indices_of_labels_to_read = Vector{Int}(undef, length(labels_to_read))
        for (i, label) in enumerate(labels_to_read)
            index = findfirst(x -> x == label, metadata.labels)
            if index === nothing
                throw(ArgumentError("Label $label not found in metadata"))
            end
            indices_of_labels_to_read[i] = index
        end

        # Fill the buffer cache and data with NaNs
        all_labels_data_cache = fill(NaN32, length(metadata.labels))
        data = fill(NaN32, length(labels_to_read))

        reader = new{I, R}(
            reader,
            filename,
            metadata,
            dimension_in_cache,
            dimension_to_read,
            all_labels_data_cache,
            data,
            labels_to_read,
            indices_of_labels_to_read,
            carrousel,
        )
        finalizer(Quiver.close!, reader)
        return reader
    end
end

function _build_dimension_to_read!(reader::Reader; dims...)
    for (i, dim) in enumerate(reader.metadata.dimensions)
        if reader.carrousel
            reader.dimension_to_read[i] = mod1(dims[dim], reader.metadata.dimension_size[i])
        else
            reader.dimension_to_read[i] = dims[dim]
        end
    end
    return nothing
end

function _build_dimension_in_cache!(reader::Reader)
    for i in 1:reader.metadata.number_of_dimensions
        reader.dimension_in_cache[i] = reader.dimension_to_read[i]
    end
    return nothing
end

function _move_data_from_buffer_cache_to_data!(reader::Reader)
    @inbounds for (i, index) in enumerate(reader.indices_of_labels_to_read)
        reader.data[i] = reader.all_labels_data_cache[index]
    end
    return nothing
end

"""
    goto!(reader::Reader; dims...)

This function moves the reader to the specified dimensions and returns the corresponding data. It updates the internal cache and retrieves the necessary time series values.

For **binary files**, `goto!` allows random access to any part of the time series, meaning you can jump between stages, scenarios, and blocks in any order. This provides greater flexibility for accessing specific points in the data.

For **CSV files**, `goto!` works differently. It only supports forward sequential access, meaning that while you can still navigate through stages, scenarios, and blocks, you cannot randomly jump to previous positions. The function moves forward through the file, reading data sequentially.

# Parameters

  - `reader::Reader`: The time series reader.
  - `dims...`: Specific dimensions to move the reader to.

# Returns

  - The data at the specified dimensions.

# Simple Example:

```julia
data = goto!(reader, stage = 1, scenario = 2, block = 5)
```
"""
function goto!(reader::Reader; dims...)
    validate_dimensions(reader.metadata, dims...)
    _build_dimension_to_read!(reader; dims...)
    _quiver_goto!(reader)
    _build_dimension_in_cache!(reader)
    _move_data_from_buffer_cache_to_data!(reader)
    return reader.data
end

"""
    next_dimension!(reader::Reader)

This function advances the reader to the next dimension and returns the updated data. It's useful when iterating over multiple dimensions sequentially. This function is especially useful for **CSV files**, where random access is not available. It allows for easy iteration through multiple dimensions in a forward-only manner.

# Parameters

  - `reader::Reader`: The time series reader.

# Returns

  - The data in the next dimension.

# Simples Example:

```julia
next_data = next_dimension!(reader)
```
"""
function next_dimension!(reader::Reader)
    _quiver_next_dimension!(reader)
    _move_data_from_buffer_cache_to_data!(reader)
    return reader.data
end

"""
    max_index(reader::Reader, dimension::String)

Returns the maximum index of the specified dimension.

# Parameters

  - `reader::Reader`: The time series reader.
  - `dimension::String`: The name of the dimension to find the index for.

# Returns

  - The maximum index of the specified dimension.
"""
function max_index(reader::Reader, dimension::String)
    symbol_dim = Symbol(dimension)
    index = findfirst(isequal(symbol_dim), reader.metadata.dimensions)
    if index === nothing
        throw(ArgumentError("Dimension $dimension not found in metadata"))
    end
    return reader.metadata.dimension_size[index]
end

"""
    close!(reader::Reader)

Closes the reader and releases associated resources.

# Parameters

  - `reader::Reader`: The time series reader to close.

# Returns

  - `nothing`.

# Simple Example:

```julia
close!(reader)
```
"""
function close!(reader::Reader)
    _quiver_close!(reader)
    return nothing
end

"""
    file_to_array(filename::String, implementation::Type{I}; labels_to_read::Vector{String} = String[]) where {I <: Implementation}

Reads a file and returns the data and metadata as a tuple.

# Parameters

  - `filename::String`: The name of the file to be read.
  - `implementation::Type{I}`: The implementation type for reading the file (binary or CSV).
  - `labels_to_read::Vector{String}`: Specific labels to read (optional).

# Returns

  - A tuple containing the read data and associated metadata.
"""
function file_to_array(
    filename::String,
    implementation::Type{I};
    labels_to_read::Vector{String} = String[],
) where {I <: Implementation}
    reader = Reader{I}(
        filename;
        labels_to_read,
    )

    metadata = reader.metadata
    dimension_names = reverse(metadata.dimensions)
    dimension_sizes = reverse(metadata.dimension_size)
    data = zeros(
        Float32,
        length(reader.labels_to_read),
        dimension_sizes...,
    )

    for dims in Iterators.product([1:size for size in dimension_sizes]...)
        dim_kwargs = OrderedDict(Symbol.(dimension_names) .=> dims)
        Quiver.goto!(reader; dim_kwargs...)
        data[:, dims...] = reader.data
    end

    Quiver.close!(reader)

    return data, metadata
end

"""
    file_to_df(filename::String, implementation::Type{I}; labels_to_read::Vector{String} = String[]) where {I <: Implementation}

Reads a file and returns the data and metadata as a DataFrame.

# Parameters

  - `filename::String`: The name of the file to be read.
  - `implementation::Type{I}`: The implementation type for reading the file (binary or CSV).
  - `labels_to_read::Vector{String}`: Specific labels to read (optional).

# Returns

  - A DataFrame with the read data and metadata.
"""
function file_to_df(
    filename::String,
    implementation::Type{I};
    labels_to_read::Vector{String} = String[],
) where {I <: Implementation}
    reader = Reader{I}(
        filename;
        labels_to_read,
    )

    metadata = reader.metadata
    dimension_names = reverse(metadata.dimensions)
    dimension_sizes = reverse(metadata.dimension_size)

    df = DataFrame()

    # Add all columns to the DataFrame
    for dim in metadata.dimensions
        DataFrames.insertcols!(df, dim => Int[])
    end
    for label in reader.labels_to_read
        DataFrames.insertcols!(df, label => Float32[])
    end

    for dims in Iterators.product([1:size for size in dimension_sizes]...)
        dim_kwargs = OrderedDict(Symbol.(dimension_names) .=> dims)
        Quiver.goto!(reader; dim_kwargs...)
        if all(isnan.(reader.data))
            continue
        end
        # Construct the data frame row by row
        push!(df, [reverse(dims)...; reader.data...])
    end

    # Add metadata to DataFrame
    orderec_dict_metadata = to_ordered_dict(metadata)
    for (k, v) in orderec_dict_metadata
        DataFrames.metadata!(df, k, v)
    end

    Quiver.close!(reader)

    return df
end
