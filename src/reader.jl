mutable struct QuiverReaderCache{N}
    dimensions_cached::Vector{Symbol}
    currently_cached_indexes::Vector{Int}
    # The cache is a column ordered N dimensional matrix of Float32
    # N is equal to the number of dimensions + the number of agents to read.
    # It should have the order that allows for the fastest access to the data
    # which is column oriented reads. For example agents is the first dimension and
    # the last one is the outer most dimension in the table.
    data::Array{Float32, N}
    # This is used to avoid allocations when building the indexes of the cache
    _currently_read_indexes::Vector{Int}
    function QuiverReaderCache(
        metadata::QuiverMetadata,
        dimensions_cached::Union{Vector{Symbol}, Nothing},
        num_agents::Int
    )
        N = num_dimensions(metadata) + 1
        max_value_per_dimension = maximum_value_of_each_dimension(metadata)
        dimensions_of_cache = [num_agents, reverse(max_value_per_dimension)...]
        dimensions_cached = if dimensions_cached === nothing
            Vector{Symbol}(undef, 0)
        else
            dimensions_cached
        end
        return new{N}(
            dimensions_cached,
            fill(typemin(Int), length(dimensions_cached)),
            fill(NaN32, dimensions_of_cache...),
            fill(0, num_dimensions(metadata)),
        )
    end
end

mutable struct QuiverReader{I <: QuiverImplementation, R, N}
    reader::Union{Nothing, R}
    filename::String
    dimensions::Vector{Symbol}
    agents_to_read::Vector{Symbol}
    metadata::QuiverMetadata
    cache::QuiverReaderCache{N}

    function QuiverReader{I, R}(
        reader::Union{Nothing, R},
        filename::String,
        dimensions::Vector{Symbol},
        agents_to_read::Vector{Symbol},
        metadata::QuiverMetadata,
        cache::QuiverReaderCache{N}
    ) where {I, R, N}
        return new{I, R, N}(
            reader,
            filename,
            dimensions,
            agents_to_read,
            metadata,
            cache
        )
    end
end

function max_index(reader::QuiverReader, dimension::String)
    index_of_dimension = findfirst(isequal(dimension), string.(reader.dimensions))
    if index_of_dimension === nothing
        error("Dimension $dimension not found in $(reader.dimensions)")
    end
    return reader.metadata.maximum_value_of_each_dimension[index_of_dimension]
end

function num_agents(reader::QuiverReader)
    return length(reader.agents_to_read)
end

function _should_update_reader_cache(reader::QuiverReader, dimensions_to_query::LittleDict{Symbol, Int})
    if isempty(reader.cache.currently_cached_indexes)
        return true
    end
    for (i, dimension) in enumerate(reader.cache.dimensions_cached)
        dimension_queried = dimensions_to_query[dimension]
        if dimension_queried != reader.cache.currently_cached_indexes[i]
            return true
        end
    end
    return false
end

function _build_dimensions_to_query_from_inds(reader::QuiverReader, inds...)
    # The first index is always the agent and the other indexes are ordered in reverse
    # from the definition in the metadata.
    dict_dimensions_to_query = LittleDict{Symbol, Int}() # LittleDict is a fast implementation of small ordered dicts
    # We skip the first index becaus it is the agent which is not a defined dimension in the file.
    for (i, ind) in enumerate(Base.Iterators.reverse(inds[2:end]))
        if isa(ind, Int)
            dict_dimensions_to_query[reader.dimensions[i]] = ind
        else
            error("Dimension $(reader.dimensions[i]) cannot be queried as $ind, only as an integer.")
        end
    end
    return dict_dimensions_to_query
end

function _fill_cache_with_df!(reader::QuiverReader, df::DataFrame)
    for row in eachrow(df)
        for (i, dimension) in enumerate(Base.Iterators.reverse(reader.dimensions))
            reader.cache._currently_read_indexes[i] = getproperty(row, dimension)
        end
        for (i, agent) in enumerate(reader.agents_to_read)
            reader.cache.data[i, reader.cache._currently_read_indexes...] = getproperty(row, agent)
        end
    end
    return nothing
end

function _update_reader_cache!(reader::QuiverReader, inds...)
    dimensions_to_query = _build_dimensions_to_query_from_inds(reader, inds...)
    if !(_should_update_reader_cache(reader, dimensions_to_query))
        return nothing
    end
    _assert_dimensions_are_in_order(reader; dimensions_to_query...)
    fill!(reader.cache.data, NaN32)
    df = _quiver_read_df(reader; dimensions_to_query...)
    _fill_cache_with_df!(reader, df)
    return nothing
end

function Base.getindex(reader::QuiverReader, inds...)
    if num_dimensions(reader.metadata) == length(inds) + 1
        error("Wrong number of dimensions.")
    end
    _update_reader_cache!(reader, inds...)
    return Base.getindex(reader.cache.data, inds...)
end

function read_df(reader::QuiverReader)
    return _quiver_read_df(reader)
end

function find_cols_of_agents(reader::QuiverReader, cols::Vector{Symbol})
    return findall((in)(reader.agents_to_read), cols)
end

function close!(writer::QuiverReader)
    _quiver_close!(writer)
    return nothing
end