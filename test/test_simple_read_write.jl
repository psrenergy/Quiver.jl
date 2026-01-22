number_of_stages = 10
number_of_scenarios = 5
number_of_blocks = 3

writer = Quiver.open_file(
    "ts1",
    "w",
    metadata=Quiver.Metadata(
        dimensions = ["stage", "scenario", "block"],
        dimension_sizes = [number_of_stages, number_of_scenarios, number_of_blocks],
        time_dimensions = ["stage", "block"],
        frequencies = ["yearly", "daily"],
        unit = "MW",
        labels = ["ger1", "ger2"],
    )
)

for stage in 1:number_of_stages
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            data = stage .+ scenario .+ block .+ [0.0, 1.0]
            Quiver.quiver_write!(
                writer,
                data;
                stage=stage,
                scenario=scenario,
                block=block,
            )
        end
    end
end

Quiver.close_file(writer)

reader = Quiver.open_file("ts1", "r")
data_arr = zeros(Float64, number_of_stages, number_of_scenarios, number_of_blocks, 2)
for stage in 1:number_of_stages
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            data = Quiver.quiver_read!(
                reader;
                stage=stage,
                scenario=scenario,
                block=block,
            )
            data_arr[stage, scenario, block, :] = data
        end
    end
end

Quiver.close_file(reader)

rm("ts1.qvr")
rm("ts1.toml")

@show data_arr