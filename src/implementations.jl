abstract type QuiverImplementation end

struct csv <: QuiverImplementation end

function implementations()::Vector{DataType}
    return [
        Quiver.csv
    ]
end