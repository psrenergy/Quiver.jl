abstract type Implementation end

struct csv <: Implementation end

function implementations()::Vector{DataType}
    return [
        Quiver.csv
    ]
end