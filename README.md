# Quiver.jl

Quiver is an alternative data-structure to represent time series data. It is designed for time series that can have extra dimensions such as scenarios, blocks, segments, etc.

Quiver is not the fastest data-structure for time series data, but it is designed to be flexible and easy to use. The main idea behind Quiver
is to have a set of dimensions that can be used to index the data and a set of values from the time serires attributes. This allows to have a
table-like data-structure that can be used to store time series data. 

Files that follow the Quiver implementation can be stored in any format that maps directly to a table-like structure with metadata.
CSV files are implemented in a way that the first few lines are used to store the metadata and the rest of the file is used to store the data., i.e.

```csv
version = 1
dimensions = ["stage", "scenario", "block"]
dimension_size = [10, 12, 744]
initial_date = "2006-01-01 00:00:00"
time_dimension = "stage"
frequency = "month"
unit = ""
labels = ["agent_1", "agent_2", "agent_3"]
--- 
stage,scenario,block,agent_1,agent_2,agent_3
1,1,1,1.0,1.0,1.0
1,1,2,1.0,1.0,1.0
1,1,3,1.0,1.0,1.0
```

The metadata stores the frequency of the time series, the initial date, the unit of the data, the number of the dimension, the maximum value of each dimension, the time dimension and the version of the file.