mutable struct QuiverWriter{I <: QuiverImplementation, W}
    writer::W
    filename::String
    dimensions::Vector{String}
    agents::Vector{String}
    metadata::QuiverMetadata
    last_dimension_added::Vector{Int32}
end

# This is something for safety but could be turn off
# It helps us guarantee that the dimensions follow their expected behaviour
# There should be smarter ways of doing these checks
function _dimensions_are_compliant(writer::QuiverWriter, dimensions::Matrix{Int32})::Bool
    # The first dimension must be grater or equal than the last one added
    first_dim = dimensions[1, :]
    for (i, dim) in enumerate(first_dim)
        if dim > writer.last_dimension_added[i]
            # If this is true then everything the order is certainly respected
            break
        elseif dim == writer.last_dimension_added[i]
            # If this is true we still need to check if the next dimension respects the order
            continue
        else # dim < writer.last_dimension_added[i]
           return false
        end
    end

    # The next element of dimensions must be grater or equal than the previous one
    for i in 1:size(dimensions, 1) - 1
        for dim in axes(dimensions, 2)
            if dimensions[i + 1, dim] > dimensions[i, dim]
                break
            elseif dimensions[i + 1, dim] == dimensions[i, dim]
                continue
            else
                return false
            end
        end
    end 

    return true
end

function write!(writer::QuiverWriter, df::DataFrames.DataFrame)
    _quiver_write!(writer, df)
    return nothing
end

"""
    write!(writer::QuiverWriter, dimensions::Matrix{I}, agents::Matrix{F}) where {I <: Integer, F <: AbstractFloat}

Writes the dimensions and agents to the file writer.
"""
function write!(
    writer::QuiverWriter,
    dimensions::Matrix{I}, 
    agents::Matrix{F}
) where {I <: Integer, F <: AbstractFloat}
    if !_dimensions_are_compliant(writer, dimensions)
        error("Dimensions are not in order.")
    end
    intermediary_df = DataFrames.DataFrame(agents, writer.agents)
    for dim in 1:length(writer.dimensions)
        DataFrames.insertcols!(intermediary_df, dim, writer.dimensions[dim] => dimensions[:, dim])
    end
    _quiver_write!(writer, intermediary_df)
    writer.last_dimension_added = dimensions[end, :]
    return nothing
end

"""
    close!(writer::QuiverWriter)

Closes the file writer.
"""
function close!(writer::QuiverWriter)
    _quiver_close!(writer)
    return nothing
end