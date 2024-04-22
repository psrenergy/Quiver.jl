# Quiver.jl

Quiver is an internal implementation of time series data structure design. 
The goal is to provide a simple way of dealing with time series data that 
can have more than simply the time dimension. Dimensions can be scenarios, 
blocks, segments or any other abstraction that a user might find necessary to be stored.

There are differente file formats that can be used to store the data, some of them are 
optimized to be memory efficient, others are optimized to be fast to write and read, and others
might be only easier to read in a text file. Each format might have is own advantages and
disadvantages, and the user should choose the one that fits better to the problem at hand.