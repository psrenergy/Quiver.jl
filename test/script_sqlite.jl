using Dates
using PSRClassesInterface
PSRI = PSRClassesInterface
include("../src/sqlite_done_right.jl")
using .SQLiteDoneRight
using Profile
using PProf

GC.gc()

function write_graf_time_series(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_agents::Int = 20,
    num_blocks::Int = 720
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

    base_values = ones(Float32, num_agents)

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                PSRI.write_registry(
                    iow,
                    base_values,
                    stage,
                    scenario,
                    block
                )
            end
        end
    end

    PSRI.close(iow)
end

function read_graf_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_blocks::Int = 720
)
    FILE_PATH = "./$file_name"
    ior = PSRI.open(
        PSRI.OpenBinary.Reader, 
        FILE_PATH;
        use_header = false
    )
    for stage = 1:num_stages
        for scenario = 1:num_scenarios
            for block = 1:num_blocks
                PSRI.goto(ior, stage, scenario, block)
                ior.data[:]
            end
        end
    end

    PSRI.close(ior)
end

function graf_file_size(;
    file_name::String = "time_series",
)
    FILE_PATH = "./$file_name.bin"
    size = filesize(FILE_PATH) / 1024 / 1024
    return round(size, digits = 2)
end

function write_sqlite_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_agents::Int = 20,
    num_blocks::Int = 720
)
    agents_names = ["Agent_$i" for i in 1:num_agents]

    iow = SQLiteDoneRight.crate_sqlite_time_series_writer(
        file_name,
        num_stages,
        num_scenarios,
        num_blocks,
        agents_names
    )

    base_values = ones(Float64, num_agents)

    
    SQLiteDoneRight.SQLite.transaction(iow.db) do
        for block in 1:num_blocks
            for scenario in 1:num_scenarios
                for stage in 1:num_stages
                    SQLiteDoneRight.write_registry(
                        iow,
                        base_values,
                        stage,
                        scenario,
                        block
                    )
                end
            end
        end
    end

    SQLiteDoneRight.SQLite.close(iow.db)
end

function read_sqlite_file(;
    file_name::String = "time_series",
    num_stages::Int = 60,
    num_scenarios::Int = 10,
    num_blocks::Int = 720
)
    ior = SQLiteDoneRight.create_sqlite_time_series_reader(file_name)
    for stage = 1:num_stages
        for scenario = 1:num_scenarios
            for block = 1:num_blocks
                SQLiteDoneRight.goto(ior, stage, scenario, block)
                ior.data[:]
            end
        end
    end
    SQLite.close(ior.db)
end

function sqlite_file_size(;
    file_name::String = "time_series",
)
    FILE_PATH = "./$file_name.sqlite"
    size = filesize(FILE_PATH) / 1024 / 1024
    return round(size, digits = 2)
end

function evaluate_all_implementations()
    GC.gc()
    rm("time_series.bin", force = true)
    rm("time_series.hdr", force = true)
    rm("time_series.sqlite", force = true)


    file_name = "time_series"
    num_stages = 30
    num_scenarios = 100
    num_agents = 20
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
        num_blocks = num_blocks
    )
    println("   Time to read")
    @time read_graf_file(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_blocks = num_blocks
    )
    graf_size = graf_file_size(;
        file_name = file_name
    )
    println("   File Size: $graf_size MB")

    println("Benchmarking SQLite")
    println("   Time to write")
    @time write_sqlite_file(;
        file_name = file_name,
        num_stages = num_stages,
        num_scenarios = num_scenarios,
        num_agents = num_agents,
        num_blocks = num_blocks
    )
    # println("   Time to read")
    # @time read_sqlite_file(;
    #     file_name = file_name,
    #     num_stages = num_stages,
    #     num_scenarios = num_scenarios,
    #     num_blocks = num_blocks
    # )
end