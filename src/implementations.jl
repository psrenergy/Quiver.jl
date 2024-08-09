abstract type Implementation end

struct csv <: Implementation end
struct binary <: Implementation end

function implementations()::Vector{DataType}
    return [
        Quiver.csv,
        Quiver.binary
    ]
end