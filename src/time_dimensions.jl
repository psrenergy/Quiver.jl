@enumx Frequencies begin
    YEARLY = 0
    MONTHLY = 1
    WEEKLY = 2
    DAILY = 3
    HOURLY = 4
end

function frequency_string_to_enum(freq_str::String)
    freq = if freq_str == "hourly"
        Frequencies.HOURLY
    elseif freq_str == "daily"
        Frequencies.DAILY
    elseif freq_str == "weekly"
        Frequencies.WEEKLY
    elseif freq_str == "monthly"
        Frequencies.MONTHLY
    elseif freq_str == "yearly"
        Frequencies.YEARLY
    else
        error("Unknown frequency: $freq_str")
    end
    return freq
end

function frequency_enum_to_string(freq::Frequencies.T)
    freq_str = if freq == Frequencies.HOURLY
        "hourly"
    elseif freq == Frequencies.DAILY
        "daily"
    elseif freq == Frequencies.WEEKLY
        "weekly"
    elseif freq == Frequencies.MONTHLY
        "monthly"
    elseif freq == Frequencies.YEARLY
        "yearly"
    end
    return freq_str
end

# Hours

const MAX_HOURS_IN_DAY = 24
const MAX_HOURS_IN_WEEK = 168 # 7 * 24
const MAX_HOURS_IN_MONTH = 744 # 31 * 24
const MAX_HOURS_IN_YEAR = 8784 # 366 * 24

const MIN_HOURS_IN_DAY = 24
const MIN_HOURS_IN_WEEK = 168 # 7 * 24
const MIN_HOURS_IN_MONTH = 672 # 28 * 24
const MIN_HOURS_IN_YEAR = 8760 # 365 * 24

# Days

const MAX_DAYS_IN_WEEK = 7
const MAX_DAYS_IN_MONTH = 31
const MAX_DAYS_IN_YEAR = 366

const MIN_DAYS_IN_WEEK = 7
const MIN_DAYS_IN_MONTH = 28
const MIN_DAYS_IN_YEAR = 365

# Weeks

const MAX_WEEKS_IN_YEAR = 53

const MIN_WEEKS_IN_YEAR = 52

# Months

const MAX_MONTHS_IN_YEAR = 12

const MIN_MONTHS_IN_YEAR = 12

function time_dimension_value_to_datetime(value::Int, freq::Frequencies.T, initial_value::Int)
    datetime_value = if freq == Frequencies.HOURLY
        Dates.Hour(value - initial_value)
    elseif freq == Frequencies.DAILY
        Dates.Day(value - initial_value)
    elseif freq == Frequencies.WEEKLY
        Dates.Week(value - initial_value)
    elseif freq == Frequencies.MONTHLY
        Dates.Month(value - initial_value)
    elseif freq == Frequencies.YEARLY
        Dates.Year(value - initial_value)
    else
        error("Unsupported frequency enum: $freq")
    end
    return datetime_value
end

function build_datetime_from_time_dimensions(current_dimensions::Vector{Int}, metadata::AbstractMetadata)
    datetime = metadata.initial_date

    for i in 1:metadata.number_of_time_dimensions
        datetime += time_dimension_value_to_datetime(
            current_dimensions[metadata.time_dimension_indexes[i]],
            metadata.frequencies[i],
            metadata.time_dimension_initial_values[i],
        )
    end

    return datetime
end

function build_datetime_string_from_time_dimensions(current_dimensions::Vector{Int}, metadata::AbstractMetadata)
    datetime = build_datetime_from_time_dimensions(current_dimensions, metadata)

    datetime_str = if any(isequal(Frequencies.HOURLY), metadata.frequencies)
        Dates.format(datetime, "yyyy-mm-ddTHH:MM:SS")
    else
        Dates.format(datetime, "yyyy-mm-dd")
    end
    return datetime_str
end

function extract_time_dimension_value_from_datetime(datetime::Dates.DateTime, freq::Frequencies.T)
    value = if freq == Frequencies.HOURLY
        Dates.hour(datetime)
    elseif freq == Frequencies.DAILY
        Dates.day(datetime)
    elseif freq == Frequencies.WEEKLY
        error("WEEKLY frequency extraction not implemented. This function should only be used for inner time dimensions.")
    elseif freq == Frequencies.MONTHLY
        Dates.month(datetime)
    elseif freq == Frequencies.YEARLY
        error("YEARLY frequency extraction not implemented. This function should only be used for inner time dimensions.")
    else
        error("Unsupported frequency enum: $freq")
    end

    return value
end

function day_of_week_from_datetime(datetime::Dates.DateTime)
    return mod1(Dates.dayofyear(datetime), MAX_DAYS_IN_WEEK)
end
