module TestCompareFiles

using Dates
using Quiver
using Test

function test_compare_files_with_different_metadata()
    filename1 = joinpath(@__DIR__, "test_compare_files1_different_metadata")
    filename2 = joinpath(@__DIR__, "test_compare_files2_different_metadata")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):(initial_date+Dates.Month(num_stages-1)))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer1 = Quiver.Writer{Quiver.binary}(
        filename1;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    writer2 = Quiver.Writer{Quiver.binary}(
        filename2;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date + Dates.Month(1),
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer1, data; stage, scenario, block)
                Quiver.write!(writer2, data; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer1)
    Quiver.close!(writer2)

    @test !Quiver.compare_files(filename1, filename2, Quiver.binary)

    rm("$filename1.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename2.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename1.toml")
    rm("$filename2.toml")

    return nothing
end

function test_compare_files_with_different_data()
    filename1 = joinpath(@__DIR__, "test_compare_files1_different_data")
    filename2 = joinpath(@__DIR__, "test_compare_files2_different_data")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):(initial_date+Dates.Month(num_stages-1)))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer1 = Quiver.Writer{Quiver.binary}(
        filename1;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    writer2 = Quiver.Writer{Quiver.binary}(
        filename2;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer1, data; stage, scenario, block)
                Quiver.write!(writer2, data .+ 1; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer1)
    Quiver.close!(writer2)

    @test !Quiver.compare_files(filename1, filename2, Quiver.binary)

    rm("$filename1.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename2.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename1.toml")
    rm("$filename2.toml")

    return nothing
end

function test_compare_equal_files()
    filename1 = joinpath(@__DIR__, "test_compare_files1_equal")
    filename2 = joinpath(@__DIR__, "test_compare_files2_equal")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):(initial_date+Dates.Month(num_stages-1)))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer1 = Quiver.Writer{Quiver.binary}(
        filename1;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    writer2 = Quiver.Writer{Quiver.binary}(
        filename2;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer1, data; stage, scenario, block)
                Quiver.write!(writer2, data; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer1)
    Quiver.close!(writer2)

    @test Quiver.compare_files(filename1, filename2, Quiver.binary)

    rm("$filename1.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename2.$(Quiver.file_extension(Quiver.binary))")
    rm("$filename1.toml")
    rm("$filename2.toml")

    return nothing
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

TestCompareFiles.runtests()

end
