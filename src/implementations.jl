abstract type QuiverImplementation end

struct arrow <: QuiverImplementation end
struct csv <: QuiverImplementation end

function implementations()::Vector{DataType}
    return [
        Quiver.arrow,
    ]
end