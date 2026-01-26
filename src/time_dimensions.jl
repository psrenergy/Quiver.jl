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

const MAX_WEEKS_IN_MONTH = 5
const MAX_WEEKS_IN_YEAR = 53

const MIN_WEEKS_IN_MONTH = 4
const MIN_WEEKS_IN_YEAR = 52

# Months

const MAX_MONTHS_IN_YEAR = 12

const MIN_MONTHS_IN_YEAR = 12
