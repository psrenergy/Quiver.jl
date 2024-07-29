module TestReadWrite

using Quiver
using Dates
using Test

function read_write_with_implementation(impl)
    filename = joinpath(@__DIR__, "test_read_write")
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date+Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_agents = 3

    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        filename,
        dimensions_names,
        agents_names,
        "stage",
        [num_stages, num_scenarios, maximum(num_blocks_per_stage)];
        initial_date = initial_date,
    )
    for stage in 1:num_stages
        i = 1
        agents = stage * ones(Float32, num_scenarios * num_blocks_per_stage[stage], num_agents)
        dimensions = Matrix{Int32}(undef, num_scenarios * num_blocks_per_stage[stage], 3)
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                dimensions[i, 1] = stage
                dimensions[i, 2] = scenario
                dimensions[i, 3] = block
                i += 1
            end
        end
        Quiver.write!(writer, dimensions, agents)
    end
    Quiver.close!(writer)

    reader = QuiverReader{impl}(filename)
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            result = Quiver.read(reader; stage = stage, scenario = scenario)
            @test size(result) == (num_blocks_per_stage[stage], num_agents)
            @test unique(result)[1] == stage
        end
    end
    return Quiver.close!(reader)
end

function read_write_with_implementation_passing_array(impl)
    filename = joinpath(@__DIR__, "test_read_write_with_implementation_passing_array")
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_agents = 3

    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        filename,
        dimensions_names,
        agents_names,
        "stage",
        [num_stages, num_scenarios, num_blocks];
        initial_date = initial_date,
    )

    agents = zeros(Float32, num_agents, num_blocks, num_scenarios)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                agents[:, block, scenario] .= stage * scenario
            end
        end
        Quiver.write!(writer, agents; stage = stage)
    end
    Quiver.close!(writer)

    reader = QuiverReader{impl}(filename)
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            result = Quiver.read(reader; stage = stage, scenario = scenario)
            @test size(result) == (num_blocks, num_agents)
            @test unique(result)[1] == stage * scenario
        end
    end
    return Quiver.close!(reader)
end

function read_write_with_implementation_passing_full_array(impl)
    filename = joinpath(@__DIR__, "test_read_write_with_implementation_passing_full_array")
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_agents = 3

    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        filename,
        dimensions_names,
        agents_names,
        "stage",
        [num_stages, num_scenarios, num_blocks];
        initial_date = initial_date,
    )

    agents = zeros(Float32, num_agents, num_blocks, num_scenarios, num_stages)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                for ag in 1:num_agents
                    agents[ag, block, scenario, stage] = ag * block * scenario * stage
                end
            end
        end
    end
    Quiver.write!(writer, agents)
    Quiver.close!(writer)

    reader = QuiverReader{impl}(filename)
    result = Quiver.read(reader)
    row = 1
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                @test agents[:, block, scenario, stage] == result[row, :]
                row += 1
            end
        end
    end
    return Quiver.close!(reader)
end

function test_read_write()
    for impl in Quiver.implementations()
        read_write_with_implementation(impl)
    end
    # Windows has some kind of problem releasing the Arrow file mmaped
    GC.gc()
    GC.gc()
    # rm(joinpath(@__DIR__, "test_read_write.arrow"))
    return rm(joinpath(@__DIR__, "test_read_write.csv"))
end

function test_read_write_with_implementation_passing_array()
    for impl in Quiver.implementations()
        read_write_with_implementation_passing_array(impl)
    end
    # Windows has some kind of problem releasing the Arrow file mmaped
    GC.gc()
    GC.gc()
    # rm(joinpath(@__DIR__, "test_read_write_with_implementation_passing_array.arrow"))
    return rm(joinpath(@__DIR__, "test_read_write_with_implementation_passing_array.csv"))
end

function test_read_write_with_implementation_passing_full_array()
    for impl in Quiver.implementations()
        read_write_with_implementation_passing_full_array(impl)
    end
    # Windows has some kind of problem releasing the Arrow file mmaped
    GC.gc()
    GC.gc()
    # rm(joinpath(@__DIR__, "test_read_write_with_implementation_passing_full_array.arrow"))
    return rm(joinpath(@__DIR__, "test_read_write_with_implementation_passing_full_array.csv"))
end

function runtests()
    Base.GC.gc()
    Base.GC.gc()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

TestReadWrite.runtests()

end