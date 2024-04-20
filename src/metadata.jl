using EnumX

@enumX TypeOfTimeColumn begin
    ExplicitDates = 1
    TimeDeltas = 2
end

mutable struct Metadata
    # frequency defines what is the time delta between 
    # two consecutive data points. It is a string that
    # can be parsed and treated internally. This is something
    # equivalent to stage_type in other applications.
    frequency::String 
    # unit is the unit of this particular time series
    unit::String
    # num_dimensions is the the number of columns a part 
    # from the time column that are other dimensions of the
    # data. The dimensions could be things like, scenarios, 
    # blocks, segments, etc.
    num_dimensions::Int
    # type_of_time_column is the type of the time column
    # that is being used. It can be either explicit dates
    # or time deltas. If it is explicit dates, then the
    # time column is a vector of dates. If it is time deltas
    # then the time column is a vector of numbers.
    type_of_time_column::TypeOfTimeColumn.T
    # initial_date is the date of the first data point. It
    # is only relevant if the type_of_time_column is
    # TimeDeltas. The initial date should be a string that
    initial_date::Union{Nothing, String}
    # date_format is the format of the date.
    date_format::String = "yyyy-mm-ddTHH:MM:SS"
end