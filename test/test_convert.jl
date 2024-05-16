module TestConvert

using Quiver
using Dates
using Test

function write_test_file(impl)
    filename = joinpath(@__DIR__, "test_convert")
    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_agents = 20
    
    # This should be a vector of symbols
    dimensions_names = ["stage", "scenario", "block"]
    agents_names = ["agent_$i" for i in 1:num_agents]

    writer = QuiverWriter{impl}(
        filename,
        dimensions_names,
        agents_names,
        "stage",
        [num_stages, num_scenarios, maximum(num_blocks_per_stage)],
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
end

function test_convert_from_arrow_to_csv()
    path_file = joinpath(@__DIR__, "test_convert")
    write_test_file(arrow)
    Quiver.convert(path_file, arrow, csv)
    reader = QuiverReader{csv}(path_file)
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            result = Quiver.read(reader; stage = stage, scenario = scenario)
            @test unique(result)[1] == stage
        end
    end
    Quiver.close!(reader)
    GC.gc()
    rm(joinpath(@__DIR__, "test_convert.arrow"))
    rm(joinpath(@__DIR__, "test_convert.csv"))
end

function test_convert_from_csv_to_arrow()
    path_file = joinpath(@__DIR__, "test_convert")
    write_test_file(csv)
    Quiver.convert(path_file, csv, arrow)
    reader = QuiverReader{arrow}(path_file)
    num_stages = Quiver.max_index(reader, "stage")
    num_scenarios = Quiver.max_index(reader, "scenario")
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            result = Quiver.read(reader; stage = stage, scenario = scenario)
            @test unique(result)[1] == stage
        end
    end
    Quiver.close!(reader)
    GC.gc()
    rm(joinpath(@__DIR__, "test_convert.arrow"))
    rm(joinpath(@__DIR__, "test_convert.csv"))
end

function test_convert_to_the_same_implementation()
    path_file = joinpath(@__DIR__, "test_convert")
    write_test_file(arrow)
    @test_throws ErrorException Quiver.convert(path_file, arrow, arrow)
    write_test_file(csv)
    @test_throws ErrorException Quiver.convert(path_file, csv, csv)
    GC.gc()
    rm(joinpath(@__DIR__, "test_convert.arrow"))
    rm(joinpath(@__DIR__, "test_convert.csv"))
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

TestConvert.runtests()

end