using Dates, Quiver

number_of_stages = 10
number_of_scenarios = 2
number_of_blocks = 31

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
        for block in 1:Dates.daysinmonth(2025, stage + 1)
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
        for block in 1:Dates.daysinmonth(2025, stage + 1)
            if stage == 1 && scenario == 1 && block < initial_block
                continue
            end
            data_in = Quiver.quiver_read!(
                reader;
                stage=stage,
                scenario=scenario,
                block=block,
            )
            data_arr[stage, scenario, block, :] = data_in
        end
    end
end

Quiver.close_file(reader)

# bin 2 CSV

cp("ts2.qvr", "ts2_agg.qvr"; force = true)
cp("ts2.toml", "ts2_agg.toml"; force = true)

Quiver.bin_to_csv("ts2_agg")
Quiver.bin_to_csv("ts2", aggregate_time_dimensions=false)


# CSV 2 bin

rm("ts2.qvr")
rm("ts2_agg.qvr")

Quiver.csv_to_bin("ts2")
Quiver.csv_to_bin("ts2_agg")

# bin 2 CSV

rm("ts2.csv")
rm("ts2_agg.csv")

Quiver.bin_to_csv("ts2_agg")
Quiver.bin_to_csv("ts2", aggregate_time_dimensions=false)