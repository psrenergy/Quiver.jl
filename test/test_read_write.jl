module TestWriter

using Test
using Quiver
using Dates

function read_write_1(impl)
    filename = joinpath(@__DIR__, "test_read_write_1")

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    dates = collect(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1))
    num_scenarios = 12
    num_blocks_per_stage = Int32.(Dates.daysinmonth.(dates) .* 24)
    num_agents = 3
    
    names_of_dimensions = ["stage", "scenario", "block"]
    names_of_time_series = ["agent_$i" for i in 1:num_agents]
    time_dimension = "stage"
    maximum_value_of_each_dimension = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

    writer = Quiver.Writer{impl}(
        filename;
        names_of_dimensions,
        names_of_time_series,
        time_dimension,
        maximum_value_of_each_dimension,
        initial_date = initial_date
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                data = [stage, scenario, block]
                Quiver.write!(writer, data; stage, scenario, block)
            end
        end
    end

    Quiver.close!(writer)

    reader = Quiver.Reader{impl}(filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks_per_stage[stage]
                Quiver.next_dimension!(reader)
                @test reader.data == [stage, scenario, block]
            end
        end
    end

    rm(filename, force = true)
end

function test_read_write_implementations()
    for impl in Quiver.implementations()
        read_write_1(impl)
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

TestWriter.runtests()

end # end module