module TestGrafExtension

using Dates
using PSRClassesInterface
using Quiver
using Test

PSRI = PSRClassesInterface

function test_graf_convertion_fixed_blocks()
    BLOCKS = 3
    SCENARIOS = 5
    STAGES = 12
    INITIAL_STAGE = 4

    FILE_PATH = joinpath(@__DIR__, "test_convert_fixed_blocks")

    for impl in Quiver.implementations()
        for stage_type in [PSRI.STAGE_MONTH, PSRI.STAGE_WEEK, PSRI.STAGE_DAY]
            iow = PSRClassesInterface.open(
                PSRClassesInterface.OpenBinary.Writer,
                FILE_PATH;
                blocks = BLOCKS,
                scenarios = SCENARIOS,
                stages = STAGES,
                agents = ["X", "Y", "Z"],
                unit = "MW",
                # optional:
                initial_stage = INITIAL_STAGE,
                initial_year = 2020,
                stage_type = stage_type,
            )

            for t in 1:STAGES, s in 1:SCENARIOS, b in 1:BLOCKS
                X = t + s + 0.0
                Y = s - t + 0.0
                Z = t + s + b * 100.0
                PSRClassesInterface.write_registry(iow, [X, Y, Z], t, s, b)
            end

            PSRClassesInterface.close(iow)

            Quiver.convert(
                FILE_PATH,
                PSRClassesInterface.OpenBinary.Reader,
                impl
            )

            # Test if data was correctly converted
            reader = Quiver.Reader{impl}(FILE_PATH)
            num_stages = reader.metadata.dimension_size[1]
            num_scenarios = reader.metadata.dimension_size[2]
            num_blocks = reader.metadata.dimension_size[3]
            for t in 1:num_stages
                for s in 1:num_scenarios
                    for b in 1:num_blocks
                        X = t + s + 0.0
                        Y = s - t + 0.0
                        Z = t + s + b * 100.0
                        if impl == Quiver.csv
                            Quiver.next_dimension!(reader)
                        else
                            Quiver.goto!(reader; stage = t, scenario = s, block = b)
                        end
                        @test reader.data == [X, Y, Z]
                    end
                end
            end

            Quiver.close!(reader)

            @test reader.metadata.labels == ["X", "Y", "Z"]
            @test reader.metadata.unit == "MW"
            @test reader.metadata.time_dimension == :stage
            @test reader.metadata.dimensions == [:stage, :scenario, :block]
        end
    end

    rm(FILE_PATH * ".bin")
    rm(FILE_PATH * ".hdr")
    rm(FILE_PATH * ".quiv")
    rm(FILE_PATH * ".csv")
    rm(FILE_PATH * ".toml")
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

TestGrafExtension.runtests()

end