"""

"""
function create_copy_in_other_format(
    filename::AbstractString,
    src_impl::QuiverImplementation,
    dest_impl::QuiverImplementation
)
    src_reader = QuiverReader{src_impl}(filename)
    dest_writer = QuiverWriter{dest_impl}(
        filename,
        src_reader.dimensions,
        src_reader.agents
    )
    # Read from the source and write to the destination
    # TODO    
    close!(src_reader)
    close!(dest_writer)
    return nothing
end