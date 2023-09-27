function graf_write_2d_one_shot(file_name::String, arr::Matrix{Float32}, agent_names::Vector{String})
    # Graf write
    iow = PSRI.open(
        PSRI.OpenBinary.Writer,
        file_name,
        blocks = 1,
        scenarios = 1,
        stages = size(arr, 1),
        agents = agent_names,
        unit = "",
        initial_stage = 1,
        initial_year = 2006,
    )

    for stage = 1:size(arr, 1), scenario = 1:1, block = 1:1
        PSRI.write_registry(
            iow,
            arr[stage, :],
            stage,
            scenario,
            block
        )
    end

    PSRI.close(iow)
    return nothing
end