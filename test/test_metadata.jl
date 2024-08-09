module TestMetadata

using Test
using Quiver
using Dates

function test_create_metadata()
    metadata = Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "stage",
        unit = "m",
        dimension_size = [10, 20],
        labels = ["ts1", "ts2", " ts3"]
    )

    @test metadata.frequency == "M"
    @test metadata.initial_date == Dates.DateTime(2000)
    @test metadata.number_of_dimensions == 2
    @test metadata.number_of_time_series == 3
end

function test_creating_invalid_metadata()
    @test_throws ErrorException Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "some_other_thing",
        unit = "m",
        dimension_size = [10, 20],
        labels = ["ts1", "ts2", " ts3"]
    )

    @test_throws ErrorException Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "stage",
        unit = "m",
        dimension_size = [10, 20, 30],
        labels = ["ts1", "ts2", " ts3"]
    )

    @test_throws ErrorException Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "stage",
        unit = "m",
        dimension_size = [10, 20],
        labels = String[]
    )

    @test_throws ErrorException Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "stage",
        unit = "m",
        dimension_size = [10, 20],
        labels = ["ts1", "ts2", "ts1"]
    )
end

function test_writing_and_reading_toml()
    metadata = Quiver.Metadata(
        frequency = "M",
        initial_date = Dates.DateTime(2000),
        dimensions = ["stage", "scenario"],
        time_dimension = "stage",
        unit = "m",
        dimension_size = [10, 20],
        labels = ["ts1", "ts2", "ts3"]
    )

    Quiver.to_toml(metadata, "test_metadata.toml")
    metadata2 = Quiver.from_toml("test_metadata.toml")

    @test metadata == metadata2

    rm("test_metadata.toml")
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

TestMetadata.runtests()

end # end module