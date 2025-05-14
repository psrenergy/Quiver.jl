module TestCompare

using Test
using Quiver
using DataFrames
using Dates

function compare1(impl)
    filename = joinpath(@__DIR__, "test_compare1")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 4
    dates = collect(initial_date:Dates.Month(1):(initial_date+Dates.Month(num_stages-1)))
    num_scenarios = 3
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    labels = ["agent_$i" for i in 1:num_time_series]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    data = zeros(num_time_series, maximum(num_blocks_per_stage), num_scenarios, num_stages)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                for i in 1:num_time_series
                    data[i, block, scenario, stage] = stage + scenario + block + i
                end
            end
        end
    end

    Quiver.array_to_file(
        filename,
        data,
        impl;
        dimensions,
        labels,
        time_dimension,
        dimension_size,
        initial_date,
    )

    @test Quiver.compare(
        filename,
        impl;
        initial_date = initial_date,
        number_of_dimensions = length(dimensions),
        dimensions = dimensions,
        time_dimension = time_dimension,
        dimension_size = dimension_size,
        number_of_time_series = num_time_series,
        labels = labels,
        data = data,
    ) == true

    @test Quiver.compare(
        filename,
        impl;
        initial_date = DateTime(2007, 1, 1),
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        number_of_dimensions = 5,
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        dimensions = ["stage", "scenario", "block", "segment"],
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        time_dimension = "segment",
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage), 1],
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        number_of_time_series = 4,
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        labels = ["agent_1", "agent_2", "agent_4"],
    ) == false

    @test Quiver.compare(
        filename,
        impl;
        data = zeros(4, maximum(num_blocks_per_stage), num_scenarios, num_stages),
    ) == false

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")

    return nothing
end

function test_compare_implementations()
    for impl in Quiver.implementations()
        @testset "Compare $(impl)" begin
            compare1(impl)
        end
    end
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

TestCompare.runtests()

end
