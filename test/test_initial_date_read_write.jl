number_of_stages = 10
number_of_scenarios = 2
number_of_blocks = 30

initial_stage = 2
initial_block = 2

last_stage = initial_stage + number_of_stages - 1

writer = Quiver.open_file(
    "ts2",
    "w",
    metadata=Quiver.Metadata(
        dimensions = ["stage", "scenario", "block"],
        dimension_sizes = [number_of_stages, number_of_scenarios, number_of_blocks],
        time_dimensions = ["stage", "block"],
        time_dimension_initial_values = [initial_stage, initial_block],
        frequencies = ["monthly", "daily"],
        unit = "MW",
        labels = ["ger1", "ger2"],
    )
)

data = [0.0, 1.0]

for stage in initial_stage:last_stage
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            if stage == initial_stage && block < initial_block
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
for (stage_idx, stage) in enumerate(initial_stage:last_stage)
    for scenario in 1:number_of_scenarios
        for block in 1:number_of_blocks
            if stage == initial_stage && scenario == 1 && block < initial_block
                continue
            end
            data = Quiver.quiver_read!(
                reader;
                stage=stage,
                scenario=scenario,
                block=block,
            )
            data_arr[stage_idx, scenario, block, :] = data
        end
    end
end

Quiver.close_file(reader)

rm("ts2.qvr")
rm("ts2.toml")

@show data_arr;