# TODO maybe this should be a struct
# TODO add compression here
mutable struct QuiverWriter
    arrow_writer::Arrow.Writer
    dimensions::Vector{Symbol}
    agents::Vector{Symbol}
    initial_date::Union{Nothing, Dates.DateTime}
    stage_type::Union{Nothing, String}
end

function create_file(
    filename::String,
    dimensions::Vector{String},
    agents::Vector{String};
    initial_date::Union{Nothing, Dates.DateTime} = nothing,
    stage_type::Union{Nothing, String} = nothing,
    remove_if_exists::Bool = true
)
    # Make a gc pass before starting
    Base.GC.gc()

    if isfile(filename)
        if remove_if_exists
            rm(filename; force = true)
        else
            error("File $filename already exists.")
        end
    end

    empty_df_quiver = DataFrames.DataFrame()
    for dim in dimensions
        empty_df_quiver[!, Symbol(dim)] = Int32[]
    end
    for agent in agents
        empty_df_quiver[!, Symbol(agent)] = Float32[]
    end

    metadata = Dict{String, String}()
    if stage_type !== nothing
        metadata["stage_type"] = stage_type
    end
    if initial_date !== nothing
        metadata["initial_date"] = string(initial_date)
    end
    metadata["dimension_columns"] = string(length(dimensions))
    metadata["agents_columns"] = string(length(agents))

    arrow_writer = open(Arrow.Writer, filename)
    Arrow.write(arrow_writer, empty_df_quiver)

    writer = QuiverWriter(
        arrow_writer, 
        Symbol.(dimensions), 
        Symbol.(agents), 
        initial_date, 
        stage_type
    )

    return writer
end

function write!(writer::QuiverWriter, df::DataFrames.DataFrame)
    Arrow.write(writer.arrow_writer, df)
    return nothing
end

function write!(writer::QuiverWriter, dimensions::Matrix{Int32}, agents::Matrix{Float32})
    intermediary_df = DataFrames.DataFrame(agents, writer.agents)
    for dim in 1:length(writer.dimensions)
        DataFrames.insertcols!(intermediary_df, dim, writer.dimensions[dim] => dimensions[:, dim])
    end
    Quiver.write!(writer, intermediary_df)
    return nothing
end

function close!(writer::QuiverWriter)
    close(writer.arrow_writer)
    return nothing
end

function columns(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Arrow.names
end

function dataframe(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> DataFrames.DataFrame
end

function metadata(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Arrow.getmetadata
end

function schema(filename::String)
    assert_file_exists(filename)
    return filename |> Arrow.Table |> Tables.schema
end

@inline function assert_file_exists(filename::String)
    @assert isfile(filename) "File $filename does not exist."
    return nothing
end

function read_dimensions(quiver_file::String, dims...)
    tbl = read_table_from_file!(quiver_file)
    return Arrow.getcolumn(tbl, dims...)
end