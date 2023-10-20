using Test

# TODO add some testset
# add a test where we dont read all agents
# adda test where we can read and write in batches
# Add a test of invalid reads
# Add a test with nn existing implementations


# Includes all .jl files inside the "test/cases" folder
function recursive_include(path::String)
    for file in readdir(path)
        file_path = joinpath(path, file)
        if isdir(file_path)
            recursive_include(file_path)
            continue
        elseif !endswith(file, ".jl")
            continue
        elseif startswith(file, "test")
            @testset "$(file)" begin
                include(file_path)
            end
        end
    end
end

recursive_include(".")