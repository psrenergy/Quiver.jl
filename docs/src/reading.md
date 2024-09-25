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
```@docs
Quiver.goto!
```

```@docs
Quiver.next_dimension!
```

```@docs
Quiver.file_to_array
```

```@docs
Quiver.file_to_df
```

#### Closing the Reader:

Always close the reader when done to release resources.

```@docs
Quiver.close!
```