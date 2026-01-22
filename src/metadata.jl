@enumx Frequencies begin
    HOURLY = 0
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4
end

mutable struct Metadata
    version::Int
    dimensions::Vector{Symbol}
    dimension_sizes::Vector{Int}
    dimension_initial_values::Vector{Int}
    number_of_dimensions::Int
    time_dimensions::Vector{Symbol}
    frequencies::Vector{Frequencies.T}
    time_dimension_initial_values::Vector{Int}
    number_of_time_dimensions::Int
    unit::String
    labels::Vector{String}
    number_of_labels::Int
end

function Metadata(;
    dimensions::Vector{String},
    dimension_sizes::Vector{Int},
    time_dimensions::Vector{String},
    frequencies::Vector{String},
    time_dimension_initial_values::Vector{Int} = ones(Int, length(time_dimensions)),
    unit::String,
    labels::Vector{String},
    version::Int = QUIVER_FILE_VERSION,
)
    dimension_initial_values = ones(Int, length(dimensions))
    for (idx, dim) in enumerate(dimensions)
        time_dim_idx = findfirst(x -> x == dim, time_dimensions)
        if time_dim_idx !== nothing
            dimension_initial_values[idx] = time_dimension_initial_values[time_dim_idx]
        end
    end
    metadata = Metadata(
        version,
        Symbol.(dimensions),
        dimension_sizes,
        dimension_initial_values,
        length(dimensions),
        Symbol.(time_dimensions),
        frequency_string_to_enum.(frequencies),
        time_dimension_initial_values,
        length(time_dimensions),
        unit,
        labels,
        length(labels),
    )

    validate_metadata(metadata)

    return metadata
end

function metadata_from_toml(filepath::String)
    metadata_filepath = filepath * ".toml"
    metadata_dict = Dict()

    for (key, value) in TOML.parsefile(metadata_filepath)
        metadata_dict[Symbol(key)] = value
    end

    metadata = Metadata(;
        metadata_dict...
    )
    validate_metadata(metadata)

    return metadata
end

function metadata_to_toml(metadata::Metadata, filepath::String)
    validate_metadata(metadata)

    metadata_dict = Dict(
        "version" => QUIVER_FILE_VERSION,
        "dimensions" => String.(metadata.dimensions),
        "dimension_sizes" => metadata.dimension_sizes,
        "time_dimensions" => String.(metadata.time_dimensions),
        "frequencies" => frequency_enum_to_string.(metadata.frequencies),
        "time_dimension_initial_values" => metadata.time_dimension_initial_values,
        "unit" => metadata.unit,
        "labels" => metadata.labels,
    )

    metadata_filepath = filepath * ".toml"
    open(metadata_filepath, "w") do io
        TOML.print(io, metadata_dict)
    end

    return nothing
end

function max_value_per_dimension(metadata::Metadata)
    max_dims = metadata.dimension_initial_values .+ metadata.dimension_sizes .- 1

    return max_dims
end

function frequency_string_to_enum(freq_str::String)
    freq = if freq_str == "hourly"
        Frequencies.HOURLY
    elseif freq_str == "daily"
        Frequencies.DAILY
    elseif freq_str == "weekly"
        Frequencies.WEEKLY
    elseif freq_str == "monthly"
        Frequencies.MONTHLY
    elseif freq_str == "yearly"
        Frequencies.YEARLY
    else
        error("Unknown frequency: $freq_str")
    end
    return freq
end

function frequency_enum_to_string(freq::Frequencies.T)
    freq_str = if freq == Frequencies.HOURLY
        "hourly"
    elseif freq == Frequencies.DAILY
        "daily"
    elseif freq == Frequencies.WEEKLY
        "weekly"
    elseif freq == Frequencies.MONTHLY
        "monthly"
    elseif freq == Frequencies.YEARLY
        "yearly"
    end
    return freq_str
end