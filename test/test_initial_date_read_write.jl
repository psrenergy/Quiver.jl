number_of_stages = 10
number_of_scenarios = 2
number_of_blocks = 30

initial_block = 2

writer = Quiver.open_file(
    "ts2",
    "w",
    metadata=Quiver.Metadata(
        dimensions = ["stage", "scenario", "block"],
        dimension_sizes = [number_of_stages, number_of_scenarios, number_of_blocks],
        time_dimensions = ["stage", "block"],
        initial_date = "2025-02-02",
        frequencies = ["monthly", "daily"],
        unit = "MW",
        labels = ["ger1", "ger2"],
    )
)

data = [0.0, 1.0]

for stage in 1:number_of_stages
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            if stage == 1 && block < initial_block
                continue
            end
            data .+= [1.0, 1.0]
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

reader = Quiver.open_file("ts2", "r")
data_arr = zeros(Float64, number_of_stages, number_of_scenarios, number_of_blocks, 2)
for stage in 1:number_of_stages
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            if stage == 1 && scenario == 1 && block < initial_block
                continue
            end
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

rm("ts2.qvr")
rm("ts2.toml")

@show data_arr;