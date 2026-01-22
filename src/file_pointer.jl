mutable struct FilePointer
    io::IO
    filepath::String
    metadata::Metadata
end

function open_file(filepath::String, mode::String="r"; metadata::Union{Metadata, Nothing}=nothing)
    validate_file_mode(mode)

    file_pointer = if mode == "r"
        validate_file_exists(filepath)
        open_reader(filepath)
    elseif mode == "w"
        open_writer(filepath, metadata)
    end

    return file_pointer
end

function close_file(file_pointer::FilePointer)
    if isopen(file_pointer.io)
        close(file_pointer.io)
    end
    return nothing
end

function open_reader(filepath::String)
    metadata = metadata_from_toml(filepath)
    io = open(filepath * ".qvr", "r")
    file_pointer = FilePointer(io, filepath, metadata)
    return file_pointer
end

function open_writer(filepath::String, metadata::Metadata)
    metadata_to_toml(metadata, filepath)
    io = open(filepath * ".qvr", "w")
    file_pointer = FilePointer(io, filepath, metadata)
    return file_pointer
end
