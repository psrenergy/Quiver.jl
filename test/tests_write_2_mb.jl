module Tests2Mb

using Test
using Random
using BenchmarkTools
import Quiver

function setup_2_mb_zeros_array()
    # File of 1000 stages and 500 agents
    n_stages = 1000
    n_agents = 500
    agent_names = ["Agent_$i" for i in 1:n_agents]
    output_2_mb_zeros = zeros(Float32, n_stages, n_agents)
    return output_2_mb_zeros, agent_names
end

function setup_2_mb_rand_array()
    # File of 1000 stages and 500 agents
    n_stages = 1000
    n_agents = 500
    agent_names = ["Agent_$i" for i in 1:n_agents]
    Random.seed!(1234)
    output_2_mb_rand = rand(Float32, n_stages, n_agents)
    return output_2_mb_rand, agent_names
end

function test_write_graf_2_mb_file()
    println("Time to write 2Mb graf file.")
    output_2_mb_zeros, agent_names = setup_2_mb_zeros_array()
    output_2_mb_rand, agent_names = setup_2_mb_rand_array()

    @btime Quiver.graf_write_2d_one_shot(joinpath(pwd(), "graf_2mb_zeros"), $output_2_mb_zeros, $agent_names)
    @btime Quiver.graf_write_2d_one_shot(joinpath(pwd(), "graf_2mb_rand"), $output_2_mb_rand, $agent_names)

    return nothing
end

function test_write_sqlite_2_mb_file()
    println("Time to write 2Mb sqlite file.")
    output_2_mb_zeros, agent_names = setup_2_mb_zeros_array()
    output_2_mb_rand, agent_names = setup_2_mb_rand_array()

    @btime Quiver.sqlite_write_2d_one_shot("sql_2mb_zeros", $output_2_mb_zeros, $agent_names)
    @btime Quiver.sqlite_write_2d_one_shot("sql_2mb_rand", $output_2_mb_rand, $agent_names)
end

function test_write_arrow_2_mb_file()
    println("Time to write 2Mb arrow file.")
    output_2_mb_zeros, agent_names = setup_2_mb_zeros_array()
    output_2_mb_rand, agent_names = setup_2_mb_rand_array()

    @btime Quiver.arrow_write_2d_one_shot("arrow_2mb_zeros", $output_2_mb_zeros, $agent_names)
    @btime Quiver.arrow_write_2d_one_shot("arrow_2mb_rand", $output_2_mb_rand, $agent_names)
end


function runtests()
    for name in names(@__MODULE__; all = true)
        if startswith("$name", "test_")
            @testset "$(name)" begin
                getfield(@__MODULE__, name)()
            end
        end
    end
end

Tests2Mb.runtests()

end