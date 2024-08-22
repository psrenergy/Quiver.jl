module PSRClassesInterfaceExt

using Dates
using Quiver
using PSRClassesInterface

const PSRI = PSRClassesInterface

function build_initial_date(initial_stage::Int, initial_year::Int, stage_type::PSRI.StageType)
    if stage_type == PSRI.STAGE_MONTH
        return DateTime(initial_year, initial_stage, 1)
    elseif stage_type == PSRI.STAGE_DAY
        return DateTime(initial_year, 1, 1) + Dates.Day(initial_stage - 1)
    elseif stage_type == PSRI.STAGE_WEEK
        return DateTime(initial_year, 1, 1) + Dates.Week(initial_stage - 1)
    else
        error("Convertion of graf file with stage type $stage_type is not supported.")
    end
end

function Quiver.convert(
    filepath::String,
    from::Type{PSRI.OpenBinary.Reader},
    to::Type{impl};
    destination_directory::String = dirname(filepath),
) where impl <: Quiver.Implementation
    filename = basename(filepath)
    destination_path = joinpath(destination_directory, filename)

    # Open graf file and read metadata
    graf_reader = PSRI.open(
        PSRI.OpenBinary.Reader,
        filepath;
        use_header = false,
    )

    stages = PSRI.max_stages(graf_reader)
    scenarios = PSRI.max_scenarios(graf_reader)
    blocks = PSRI.max_blocks(graf_reader)
    stage_type = PSRI.stage_type(graf_reader)
    initial_stage = PSRI.initial_stage(graf_reader)
    initial_year = PSRI.initial_year(graf_reader)
    agent_names = PSRI.agent_names(graf_reader)
    unit = PSRI.data_unit(graf_reader)

    initial_date = build_initial_date(initial_stage, initial_year, stage_type)

    writer = Quiver.Writer{impl}(
        destination_path;
        dimensions = ["stage", "scenario", "block"],
        labels = agent_names,
        time_dimension = "stage",
        dimension_size = [stages, scenarios, blocks],
        initial_date = initial_date,
        unit = unit,
    )

    for t in 1:stages, s in 1:scenarios, b in 1:PSRI.blocks_in_stage(graf_reader, t)
        PSRI.goto(graf_reader, t, s, b)
        Quiver.write!(writer, graf_reader[:]; stage = t, scenario = s, block = b)
    end

    Quiver.close!(writer)
    PSRI.close(graf_reader)

    return nothing
end

end