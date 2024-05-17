mutable struct QuiverWriter{I <: QuiverImplementation, W}
    writer::W
    filename::String
    dimensions::Vector{String}
    agents::Vector{String}
    metadata::QuiverMetadata
    last_dimension_added::Vector{Integer}
end

# This is something for safety but could be turn off
# It helps us guarantee that the dimensions follow their expected behaviour
# There should be smarter ways of doing these checks
function _dimensions_are_compliant(writer::QuiverWriter, dimensions::Matrix{I})::Bool where I <: Integer
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
                "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is " *
                "greater than the maximum value of the dimension $(writer.metadata.maximum_value_of_each_dimension[dim])."
            )
            return false
        else # dim < writer.last_dimension_added[i]
            @error(
                "Dimension $(metadata.dimension_names[dim]) of value $(dimensions[i + 1, dim]) is " *
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

function _agents_are_compliant(writer::QuiverWriter, agents::Matrix{F}) where {F <: AbstractFloat}
    if length(writer.agents) != size(agents, 2)
        return false
    end
    return true
end

function write!(writer::QuiverWriter, df::DataFrames.DataFrame)
    _quiver_write!(writer, df)
    return nothing
end

function _create_matrix_of_dimension_to_write(writer::QuiverWriter; provided_dimensions...)
    _assert_dimensions_are_in_order(writer; provided_dimensions...)
    # Create a matrix of Integers with the dimensions to write
    dimensions_provided_by_user = values(provided_dimensions)
    indexes_of_dimensions_provided = 1:length(dimensions_provided_by_user)
    indexes_of_dimensions_missing = length(dimensions_provided_by_user) + 1:num_dimensions(writer.metadata)
    # Get the maximum of each dimension missing
    max_dimension_per_not_provided_dimension = writer.metadata.maximum_value_of_each_dimension[indexes_of_dimensions_missing]
    number_of_rows = prod(max_dimension_per_not_provided_dimension)
    dimensions = zeros(Int32, number_of_rows, num_dimensions(writer.metadata))

    # Fill the dimensions matrix with the values provided
    for j in indexes_of_dimensions_provided
        dimensions[:, j] .= dimensions_provided_by_user[j]
    end

    # Fill the other dimensions 
    current_missing_dimensions = ones(Int32, length(indexes_of_dimensions_missing))
    reversed_indexes_of_dimensions = reverse(indexes_of_dimensions_missing)
    dimensions_offset = length(indexes_of_dimensions_provided)
    for i in 1:number_of_rows
        dimensions[i, indexes_of_dimensions_missing] .= current_missing_dimensions
        for j in reversed_indexes_of_dimensions
            dim = j - dimensions_offset
            if current_missing_dimensions[dim] < max_dimension_per_not_provided_dimension[dim]
                current_missing_dimensions[dim] += 1
                break
            else
                current_missing_dimensions[dim] = 1   
            end
        end
    end

    return dimensions
end

function write!(writer::QuiverWriter, agents::Array{F, N}; provided_dimensions...) where {F <: AbstractFloat, N}
    _assert_dimensions_are_in_order(writer; provided_dimensions...)
    # Create a matrix of Integers with the dimensions to write
    dimensions_provided_by_user = values(provided_dimensions)
    indexes_of_dimensions_missing = length(dimensions_provided_by_user) + 1:num_dimensions(writer.metadata)
    # Get the maximum of each dimension missing
    max_dimension_per_not_provided_dimension = writer.metadata.maximum_value_of_each_dimension[indexes_of_dimensions_missing]
    number_of_rows = prod(max_dimension_per_not_provided_dimension)
    agent_array_sizes = size(agents)
    number_of_agents = agent_array_sizes[1]

    # Check if agent array sizes are compatible with the dimensions we need to provide.
    for (i, s) in enumerate(agent_array_sizes[end:-1:2])
        if s != max_dimension_per_not_provided_dimension[i]
            error("Expected dimensions are $([max_dimension_per_not_provided_dimension; number_of_agents]), provided array has $(agent_array_sizes).")
        end
    end
    
    # Reshape the array into a matrix in order to be mapped to a DataFrame in the correct dimensions
    number_of_rows = prod(max_dimension_per_not_provided_dimension)

    # Build dimensions matrix
    dimensions = _create_matrix_of_dimension_to_write(writer; provided_dimensions...)
    matrix_agents = zeros(Float32, number_of_rows, number_of_agents)
    correct_index = zeros(Int, length(indexes_of_dimensions_missing))
    for ag in 1:number_of_agents
        for i in 1:number_of_rows
            for (j, dim) in enumerate(Iterators.reverse(indexes_of_dimensions_missing))
                correct_index[j] = dimensions[i, dim]
            end
            matrix_agents[i, ag] = agents[ag, correct_index...]
        end
    end

    # Pass to the next function to build the dimensions and write it.
    Quiver.write!(writer, dimensions, matrix_agents)
    return nothing
end

function write!(writer::QuiverWriter, dimensions::Matrix{I}, agents::Matrix{F}) where {I <: Integer, F <: AbstractFloat}
    if size(dimensions, 1) != size(agents, 1)
        CSV.write("dimensions.csv", DataFrame(dimensions, :auto))
        CSV.write("agents.csv", DataFrame(agents, :auto))
        error(
            "The matrix of dimensions if not the same as the matrix of agents. " *
            "The dimensions and agents matrices were saved in dimensions.csv and agents.csv, respectively."
        )
    end
    if !_dimensions_are_compliant(writer, dimensions)
        error("Dimensions are invalid.")
    end
    if !_agents_are_compliant(writer, agents)
        error("Agents are invalid.")
    end
    intermediary_df = DataFrames.DataFrame(Float32.(agents), writer.agents)
    for dim in 1:length(writer.dimensions)
        DataFrames.insertcols!(intermediary_df, dim, writer.dimensions[dim] => Int32.(dimensions[:, dim]))
    end
    _quiver_write!(writer, intermediary_df)
    writer.last_dimension_added .= dimensions[end, :]
    return nothing
end

function close!(writer::QuiverWriter)
    _quiver_close!(writer)
    return nothing
end