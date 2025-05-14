module Quiver

using CSV
using DataFrames
using Dates
using OrderedCollections
using Tables
using TOML

const QUIVER_FILE_VERSION = 1
const DEFAULT_ATOL = 1e-6
const DEFAULT_RTOL = 1e-6

include("metadata.jl")
include("utils.jl")

include("implementations.jl")

include("writer.jl")
include("reader.jl")

include("csv.jl")
include("binary.jl")

include("merge.jl")
include("operations.jl")

end
