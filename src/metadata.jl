using EnumX

Base.@kwdef mutable struct QuiverMetadata
    # frequency defines what is the time delta between 
    # two consecutive data points. It is a string that
    # can be parsed and treated internally. This is something
    # equivalent to stage_type in other applications.
    frequency::String
    # The unit of this particular time series
    unit::String = ""
    # num_dimensions is the the number of columns a part 
    # from the time column that are other dimensions of the
    # data. The dimensions could be things like, scenarios, 
    # blocks, segments, etc.
    num_dimensions::Int
    # initial_date is the date of the first data point. It
    # is only relevant if the type_of_time_column is
    # TimeDeltas. The initial date should be a string that
    initial_date::Union{Nothing, Dates.DateTime} = nothing
end