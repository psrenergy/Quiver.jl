using Quiver

path = joinpath(tempdir(), "mwe_1900")

input_datetime = "1900-01-01T00:00:00" 
md = Quiver.Binary.Metadata(;
    initial_datetime = input_datetime,
    unit = "MW",
    labels = ["plant_1", "plant_2"],
    dimensions = ["stage", "scenario"],
    dimension_sizes = Int64[12, 3],
    time_dimensions = ["stage"],
    frequencies = ["monthly"],
)

@show input_datetime # "1900-01-01T00:00:00"
@show Quiver.Binary.get_initial_datetime(md) # "1969-12-31T23:59:59"

