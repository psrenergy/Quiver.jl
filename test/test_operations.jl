module TestOperations

using Dates
using Quiver
using Test

function sum_multiple_files(impl)
    filename = joinpath(@__DIR__, "test_sum")
    filenames = [filename * "_$i" for i in 1:3]
    num_files = length(filenames)

    initial_date = DateTime(2024, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1", "agent_2", "agent_1"]

    for file in 1:num_files
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels = [labels[file]],
            time_dimension,
            dimension_size,
            initial_date = initial_date,
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end
        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_summed")
    Quiver.apply_expression(
        output_filename,
        filenames,
        +,
        impl,
    )

    reader = Quiver.Reader{impl}(output_filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [stage + block, scenario]
            end
        end
    end

    Quiver.close!(reader)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end
    rm("$output_filename.$(Quiver.file_extension(impl))")
    rm("$output_filename.toml")

    return nothing
end

function sum_block_dimension(impl)
    filename = joinpath(@__DIR__, "test_sum")

    initial_date = DateTime(2024, 1, 1)
    num_stages = 3
    num_scenarios = 2
    num_blocks = 4

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1"]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels = labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                data = (stage + scenario) * block
                Quiver.write!(writer, [data]; stage, scenario, block)
            end
        end
    end
    Quiver.close!(writer)

    output_filename = joinpath(@__DIR__, "test_summed_dimension")
    Quiver.apply_expression_over_dimension(
        output_filename,
        filename,
        sum,
        :block,
        impl,
    )

    reader = Quiver.Reader{impl}(output_filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            Quiver.goto!(reader; stage, scenario)
            @test reader.data == [(stage + scenario) * num_blocks * (num_blocks + 1) / 2]
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
    rm("$output_filename.$(Quiver.file_extension(impl))")
    rm("$output_filename.toml")

    return nothing
end

function sum_agents_in_file(impl)
    filename = joinpath(@__DIR__, "test_sum_agents")

    initial_date = DateTime(2024, 1, 1)
    num_stages = 3
    num_scenarios = 2
    num_blocks = 4

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1", "agent_2", "agent_3"]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels = labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                data = [stage, scenario, block]
                Quiver.write!(writer, data; stage, scenario, block)
            end
        end
    end
    Quiver.close!(writer)

    output_filename = joinpath(@__DIR__, "test_summed_agents")
    Quiver.apply_expression_over_agents(
        output_filename,
        filename,
        sum,
        ["agent_sum"],
        impl,
    )

    reader = Quiver.Reader{impl}(output_filename)
    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                Quiver.goto!(reader; stage, scenario, block)
                @test reader.data == [stage + scenario + block]
            end
        end
    end

    Quiver.close!(reader)

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")
    rm("$output_filename.$(Quiver.file_extension(impl))")
    rm("$output_filename.toml")

    return nothing
end

function sum_agents_error_in_file(impl)
    filename = joinpath(@__DIR__, "test_sum_agents")

    initial_date = DateTime(2024, 1, 1)
    num_stages = 3
    num_scenarios = 2
    num_blocks = 4

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1", "agent_2", "agent_3"]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels = labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                data = [stage, scenario, block]
                Quiver.write!(writer, data; stage, scenario, block)
            end
        end
    end
    Quiver.close!(writer)

    output_filename = joinpath(@__DIR__, "test_summed_agents")
    # Error because the number of labels is 2 and the operation returns 1
    @test_throws ArgumentError Quiver.apply_expression_over_agents(
        output_filename,
        filename,
        sum,
        ["agent_sum1", "agent_sum2"],
        impl,
    )

    # Error because the number of labels is 1 and the operation returns 2
    @test_throws ArgumentError Quiver.apply_expression_over_agents(
        output_filename,
        filename,
        x -> [x[1] + x[2], x[3]],
        ["agent_sum"],
        impl,
    )

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")

    return nothing
end

function sum_dimension_size_error(impl)
    filename = joinpath(@__DIR__, "test_sum_dimension_size_error")
    num_files = 3
    filenames = ["$(filename)_$(i)" for i in 1:num_files]

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]

    for file in 1:num_files
        labels = ["agent_$file"]
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels,
            time_dimension,
            dimension_size = dimension_size .+ file,
            initial_date = initial_date,
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block + scenario][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end

        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_sum_dimension_size_error_sumd")
    @test_throws ArgumentError Quiver.apply_expression(output_filename, filenames, +, impl)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end

    return nothing
end

function sum_time_dimension_error(impl)
    filename = joinpath(@__DIR__, "test_sum_time_dimension_error")
    num_files = 3
    filenames = ["$(filename)_$(i)" for i in 1:num_files]

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]

    for file in 1:num_files
        labels = ["agent_$file"]
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions = dimensions .* "$file",
            labels,
            time_dimension = time_dimension .* "$file",
            dimension_size,
            initial_date = initial_date,
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block + scenario][file]
                    dims = Quiver.OrderedDict(
                        Symbol("stage" * "$file") => stage,
                        Symbol("scenario" * "$file") => scenario,
                        Symbol("block" * "$file") => block,
                    )
                    Quiver.write!(writer, [data]; dims...)
                end
            end
        end

        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_sum_time_dimension_error_sum")
    @test_throws ArgumentError Quiver.apply_expression(output_filename, filenames, +, impl)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end

    return nothing
end

function sum_initial_date_error(impl)
    filename = joinpath(@__DIR__, "test_sum_initial_date_error")
    num_files = 3
    filenames = ["$(filename)_$(i)" for i in 1:num_files]

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]

    for file in 1:num_files
        labels = ["agent_$file"]
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels,
            time_dimension,
            dimension_size,
            initial_date = initial_date + Dates.Day(file),
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block + scenario][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end

        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_sum_initial_date_error_sum")
    @test_throws ArgumentError Quiver.apply_expression(output_filename, filenames, +, impl)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end

    return nothing
end

function sum_unit_error(impl)
    filename = joinpath(@__DIR__, "test_sum_unit_error")
    num_files = 3
    filenames = ["$(filename)_$(i)" for i in 1:num_files]

    initial_date = DateTime(2006, 1, 1)
    num_stages = 10
    num_scenarios = 12
    num_blocks = 24
    num_time_series = 3

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]

    for file in 1:num_files
        labels = ["agent_$file"]
        writer = Quiver.Writer{impl}(
            filenames[file];
            dimensions,
            labels,
            time_dimension,
            dimension_size,
            initial_date = initial_date,
            unit = "m$file",
        )

        for stage in 1:num_stages
            for scenario in 1:num_scenarios
                for block in 1:num_blocks
                    data = [stage, scenario, block + scenario][file]
                    Quiver.write!(writer, [data]; stage, scenario, block)
                end
            end
        end

        Quiver.close!(writer)
    end

    output_filename = joinpath(@__DIR__, "test_sum_unit_error_sum")
    @test_throws ArgumentError Quiver.apply_expression(output_filename, filenames, +, impl)

    for filename in filenames
        rm("$filename.$(Quiver.file_extension(impl))")
        rm("$filename.toml")
    end

    return nothing
end

function sum_errors(impl)
    filename = joinpath(@__DIR__, "test_sum")

    initial_date = DateTime(2024, 1, 1)
    num_stages = 3
    num_scenarios = 2
    num_blocks = 4

    dimensions = ["stage", "scenario", "block"]
    time_dimension = "stage"
    dimension_size = [num_stages, num_scenarios, num_blocks]
    labels = ["agent_1"]

    writer = Quiver.Writer{impl}(
        filename;
        dimensions,
        labels = labels,
        time_dimension,
        dimension_size,
        initial_date = initial_date,
    )

    for stage in 1:num_stages
        for scenario in 1:num_scenarios
            for block in 1:num_blocks
                data = (stage + scenario) * block
                Quiver.write!(writer, [data]; stage, scenario, block)
            end
        end
    end
    Quiver.close!(writer)

    output_filename = joinpath(@__DIR__, "test_summed_dimension")
    @test_throws ArgumentError Quiver.apply_expression_over_dimension(
        output_filename,
        filename,
        +,
        :period,
        impl,
    )

    @test_throws ArgumentError Quiver.apply_expression_over_dimension(
        output_filename,
        filename,
        +,
        :profile,
        impl,
    )

    rm("$filename.$(Quiver.file_extension(impl))")
    rm("$filename.toml")

    return nothing
end

function test_operations()
    for impl in Quiver.implementations()
        sum_multiple_files(impl)
        sum_block_dimension(impl)
        sum_agents_in_file(impl)
        sum_agents_error_in_file(impl)
        sum_dimension_size_error(impl)
        sum_time_dimension_error(impl)
        sum_initial_date_error(impl)
        sum_unit_error(impl)
        sum_errors(impl)
    end
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

TestOperations.runtests()

end
