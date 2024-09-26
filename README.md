# Quiver.jl

[build-img]: https://github.com/psrenergy/Quiver.jl/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/psrenergy/Quiver.jl/actions?query=workflow%3ACI

[codecov-img]: https://codecov.io/gh/psrenergy/Quiver.jl/coverage.svg?branch=master
[codecov-url]: https://codecov.io/gh/psrenergy/Quiver.jl?branch=master

| **Build Status** | **Coverage** | **Documentation** |
|:-----------------:|:-----------------:|:-----------------:|
| [![Build Status][build-img]][build-url] | [![Codecov branch][codecov-img]][codecov-url] |[![](https://img.shields.io/badge/docs-latest-blue.svg)](https://psrenergy.github.io/Quiver.jl/dev/)

Quiver is an alternative data-structure to represent time series data. It is designed for time series that can have extra dimensions such as scenarios, blocks, segments, etc.

Quiver is not the fastest data-structure for time series data, but it is designed to be flexible and easy to use. The main idea behind Quiver
is to have a set of dimensions that can be used to index the data and a set of values from the time serires attributes. This allows to have a
table-like data-structure that can be used to store time series data. 

Files that follow the Quiver implementation can be stored in any format that maps directly to a table-like structure with metadata. The metadata stores the frequency of the time series, the initial date, the unit of the data, the number of the dimension, the maximum value of each dimension, the time dimension and the version of the file.

The metadata is always stored in a TOML file in the following format:

```toml
version = 1
dimensions = ["stage", "scenario", "block"]
dimension_size = [10, 12, 744]
initial_date = "2006-01-01 00:00:00"
time_dimension = "stage"
frequency = "month"
unit = ""
labels = ["agent_1", "agent_2", "agent_3"]
```

And the data is stored in a csv or binary file that contains the values of the time series. The csv format is as follows:
```csv
stage,scenario,block,agent_1,agent_2,agent_3
1,1,1,1.0,1.0,1.0
1,1,2,1.0,1.0,1.0
1,1,3,1.0,1.0,1.0
```
