using Dates
using PSRClassesInterface
PSRI = PSRClassesInterface
using Quiver
using Profile
using PProf

GC.gc()

function write_graf_time_series(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_agents::Int = 20,
    num_blocks::Int = 720,
)
    agents_names = ["Agent_$i" for i in 1:num_agents]

    # test with PSRI
    FILE_PATH = "./$file_name"
    iow = PSRI.open(
        PSRI.OpenBinary.Writer,
        FILE_PATH,
        scenarios = num_scenarios,
        blocks = 720,
        stages = num_stages,
        agents = agents_names,
        unit = "",
        initial_stage = 1,
        initial_year = 2006,
    )

    base_values = rand(Float32, num_agents)

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                PSRI.write_registry(
                    iow,
                    base_values,
                    stage,
                    scenario,
                    block,
                )
            end
        end
    end

    return PSRI.close(iow)
end

function read_graf_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_blocks::Int = 720,
)
    FILE_PATH = "./$file_name"
    ior = PSRI.open(
        PSRI.OpenBinary.Reader,
        FILE_PATH;
        use_header = false,
    )
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                PSRI.goto(ior, stage, scenario, block)
            end
        end
    end

    return PSRI.close(ior)
end

function graf_file_size(;
    file_name::String = "time_series",
)
    FILE_PATH = "./$file_name.bin"
    size = filesize(FILE_PATH) / 1024 / 1024
    return round(size, digits = 2)
end

function write_quiver_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_agents::Int = 20,
    num_blocks::Int = 720,
)
    agents_names = ["Agent_$i" for i in 1:num_agents]

    iow = Quiver.Writer{Quiver.binary}(
        file_name;
        dimensions = ["stage", "scenario", "block"],
        labels = agents_names,
        time_dimension = "stage",
        dimension_size = [num_stages, num_scenarios, num_blocks],
        initial_date = DateTime(2006)
    )

    base_values = rand(Float64, num_agents)

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.write!(
                    iow,
                    base_values;
                    stage = stage,
                    scenario = scenario,
                    block = block,
                )
            end
        end
    end

    return Quiver.close!(iow)
end

function read_quiver_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_blocks::Int = 720,
)
    ior = Quiver.Reader{Quiver.binary}(file_name)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.goto!(ior; stage = stage, scenario = scenario, block = block)
            end
        end
    end
    return Quiver.close!(ior)
end

function quiver_file_size(;
    file_name::String = "time_series",
)
    FILE_PATH = "./$file_name.quiv"
    size = filesize(FILE_PATH) / 1024 / 1024
    return round(size, digits = 2)
end

function evaluate_all_implementations()
    GC.gc()
    rm("time_series.bin", force = true)
    rm("time_series.hdr", force = true)
    rm("time_series.quiver", force = true)

    file_name = "time_series"
    num_stages = 60
    num_scenarios = 150
    num_agents = 150
    num_blocks = 720
    println(""" 
    Dimenions: 
    num_stages: $num_stages
    num_scenarios: $num_scenarios
    num_blocks: $num_blocks
    num_agents: $num_agents
    """)

    println("Benchmarking GRAF")
    println("   Time to write")
    @time write_graf_time_series(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_agents = num_agents,
        num_blocks = num_blocks,
    )
    println("   Time to read")
    @time read_graf_file(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_blocks = num_blocks,
    )
    graf_size = graf_file_size(;
        file_name = file_name,
    )
    println("   File Size: $graf_size MB")

    println("Benchmarking Quiver")
    println("   Time to write")
    @time write_quiver_file(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_agents = num_agents,
        num_blocks = num_blocks
    )
    println("   Time to read")
    @time read_quiver_file(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_blocks = num_blocks
    )
    quiver_size = quiver_file_size(;
        file_name = file_name
    )
    println("   File Size: $quiver_size MB")

    return 
end