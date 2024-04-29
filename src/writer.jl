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
function dimensions_are_compliant(writer::QuiverWriter, dimensions::Matrix{Int32})::Bool
    # The first dimension must be grater or equal than the last one added
    first_dim = dimensions[1, :]
    for (i, dim) in enumerate(first_dim)
        if dim > writer.last_dimension_added[i]
            # If this is true then everything the order is certainly respected
            break
        elseif dim == writer.last_dimension_added[i]
            # If this is true we still need to check if the next dimension respects the order
            continue
        elseif dim > writer.metadata.maximum_value_of_each_dimension[i]
            @error(
                "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is "*
                "greater than the maximum value of the dimension $(writer.metadata.maximum_value_of_each_dimension[dim])."
            )
            return false
        else # dim < writer.last_dimension_added[i]
            @error(
                "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is "*
                "smaller than the last dimension added $(writer.last_dimension_added[i])."
            )
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
            elseif dimensions[i + 1, dim] > writer.metadata.maximum_value_of_each_dimension[dim]
                @error(
                    "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is "*
                    "greater than the maximum value of the dimension $(writer.metadata.maximum_value_of_each_dimension[dim])."
                )
                return false
            else
                @error(
                    "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is "*
                    "smaller than the last dimension added $(writer.last_dimension_added[i])."
                )
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

function write!(writer::QuiverWriter, dimensions::Matrix{Int32}, agents::Matrix{Float32})
    if !dimensions_are_compliant(writer, dimensions)
        error("Dimensions are invalid.")
    end
    intermediary_df = DataFrames.DataFrame(agents, writer.agents)
    for dim in 1:length(writer.dimensions)
        DataFrames.insertcols!(intermediary_df, dim, writer.dimensions[dim] => dimensions[:, dim])
    end
    _quiver_write!(writer, intermediary_df)
    writer.last_dimension_added = dimensions[end, :]
    return nothing
end

function close!(writer::QuiverWriter)
    _quiver_close!(writer)
    return nothing
end