## Reading

To read time series with Quiver, the Reader structure is used to manage the file, data, and dimensions. This structure helps load the relevant data from time series files, which can be either in CSV or binary format. Below is a more detailed example of how to use the `Reader`:

#### Example of initializing a Reader:

```julia
using Quiver

# Path to the time series file
filename = "path/to/your/timeseries_file"

# Initialize the Reader (assuming binary format for simplicity)
reader = Reader{Quiver.binary}(filename)

# Fetch data from the reader by specifying the stage, scenario, and block
data = goto!(reader, stage=1, scenario=2, block=5)

# Display the retrieved data
println(data)
```

### Key Functions:
#### `goto!`

This function moves the reader to the specified dimensions and returns the corresponding data. It updates the internal cache and retrieves the necessary time series values.

For **binary files**, `goto!` allows random access to any part of the time series, meaning you can jump between stages, scenarios, and blocks in any order. This provides greater flexibility for accessing specific points in the data.

For **CSV files**, `goto!` works differently. It only supports forward sequential access, meaning that while you can still navigate through stages, scenarios, and blocks, you cannot randomly jump to previous positions. The function moves forward through the file, reading data sequentially.

```julia
data = goto!(reader, stage=1, scenario=2, block=5)
```

#### `next_dimension!`

This function advances the reader to the next dimension and returns the updated data. It's useful when iterating over multiple dimensions sequentially. This function is especially useful for **CSV files**, where random access is not available. It allows for easy iteration through multiple dimensions in a forward-only manner.

```julia
next_data = next_dimension!(reader)
```
#### Closing the Reader:

Always close the reader when done to release resources.

```julia
close!(reader)
```