module SmallTimeSeries

using Quiver
using Test

function small_time_series()
    dimensions_names = ["stage", "scenario"]
    agents_names = ["agent_$i" for i in 1:20]
    dimensions = [ones(Int32, 100) collect(Int32, 1:100)]
    agents = hcat([collect(Float32, i:i+100-1) for i in 1:20]...)
    return (
        dimensions_names,
        agents_names,
        dimensions,
        agents,
    )
end

function test_write_read_small_time_series()
    (
        dimensions_names,
        agents_names,
        dimensions,
        agents,
    ) = small_time_series()

    filename = "test_small_time_series"

    for impl in Quiver.implementations()
        writer = QuiverWriter{impl}(
            filename,
            dimensions_names,
            agents_names
        )
        Quiver.write!(writer, dimensions, agents)
        Quiver.close!(writer)

        reader = QuiverReader{impl}(filename)
        data = Quiver.read(reader, (;stage = 1, scenario = 34))
        @test typeof(data) == Matrix{Float32}
        @test data[1] == 34
        @test data[8] == 41

        data = Quiver.read(reader, (;stage = 1))
        @test typeof(data) == Matrix{Float32}
        @test size(data) == size(agents)
        @test data[1, 1] == 1
        @test data[34, 1] == 34
        @test data[34, 8] == 41
    end
end

function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

SmallTimeSeries.runtests()

end # end module