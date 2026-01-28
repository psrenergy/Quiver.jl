using Dates, Quiver

number_of_stages = 1000
# number_of_stages = 10
number_of_scenarios = 1000
# number_of_scenarios = 2
number_of_blocks = 31

initial_block = 2

initial_time = time()

function test_write()

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
            for block in 1:Dates.daysinmonth(2025, mod1(stage + 1, 12))
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

    # write_time = time() - initial_time
    # println("Write time: ", write_time, " seconds")

end

function test_read()

    reader = Quiver.open_file("ts2", "r")
    # data_arr = zeros(Float64, number_of_stages, number_of_scenarios, number_of_blocks, 2)
    data_arr = zeros(Float64, 2, number_of_blocks, number_of_scenarios, number_of_stages)
    for stage in 1:number_of_stages
        for scenario in 1:number_of_scenarios
            for block in 1:Dates.daysinmonth(2025, mod1(stage + 1, 12))
                if stage == 1 && scenario == 1 && block < initial_block
                    continue
                end
                data_in = Quiver.quiver_read!(
                    reader;
                    stage=stage,
                    scenario=scenario,
                    block=block,
                )
                # data_arr[stage, scenario, block, :] = data_in
                data_arr[:, block, scenario, stage] = data_in
            end
        end
    end

    Quiver.close_file(reader)

    # read_time = time() - initial_time - write_time
    # println("Reading time: ", read_time, " seconds")

end

# exit script before conversions
function test_conversions()

    # bin 2 CSV

    cp("ts2.qvr", "ts2_agg.qvr"; force = true)
    cp("ts2.toml", "ts2_agg.toml"; force = true)

    Quiver.bin_to_csv("ts2_agg")
    Quiver.bin_to_csv("ts2", aggregate_time_dimensions=false)

    # first_convert_time = time() - initial_time - write_time - read_time
    # println("First conversion time (bin to CSV): ", first_convert_time, " seconds")

    # CSV 2 bin

    rm("ts2.qvr")
    rm("ts2_agg.qvr")

    Quiver.csv_to_bin("ts2")
    Quiver.csv_to_bin("ts2_agg")

    # second_convert_time = time() - initial_time - write_time - read_time - first_convert_time
    # println("Second conversion time (CSV to bin): ", second_convert_time, " seconds")

    # bin 2 CSV

    rm("ts2.csv")
    rm("ts2_agg.csv")

    Quiver.bin_to_csv("ts2_agg")
    Quiver.bin_to_csv("ts2", aggregate_time_dimensions=false)

    # third_convert_time = time() - initial_time - write_time - read_time - first_convert_time - second_convert_time
    # println("Third conversion time (bin to CSV): ", third_convert_time, " seconds")
end

test_write()
test_read()