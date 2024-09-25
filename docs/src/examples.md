## Examples

Here are some practical examples demonstrating how to use Quiver for time series data operations, such as writing, reading, merging, and converting between formats.

### 1. Writing and Reading Time Series

This example shows how to write and read time series data with Quiver, using multiple dimensions like stage, scenario, and block.

```julia
using Quiver
using Dates

# Define the dimensions and metadata
filename = "path/to/output/file"
initial_date = DateTime(2006, 1, 1)
num_stages = 10
num_scenarios = 12
num_blocks_per_stage = Int32.(Dates.daysinmonth.(initial_date:Dates.Month(1):initial_date + Dates.Month(num_stages - 1)) .* 24)
dimensions = ["stage", "scenario", "block"]
labels = ["agent_1", "agent_2", "agent_3"]
time_dimension = "stage"
dimension_size = [num_stages, num_scenarios, maximum(num_blocks_per_stage)]

# Initialize the Writer
writer = Quiver.Writer{Quiver.binary}(
    filename;
    dimensions,
    labels,
    time_dimension,
    dimension_size,
    initial_date = initial_date
)

# Write data
for stage in 1:num_stages
    for scenario in 1:num_scenarios
        for block in 1:num_blocks_per_stage[stage]
            data = [stage, scenario, block]
            Quiver.write!(writer, data, stage=stage, scenario=scenario, block=block)
        end
    end
end

# Close the writer
Quiver.close!(writer)

# Now, read the data back
reader = Quiver.Reader{Quiver.binary}(filename)

for stage in 1:num_stages
    for scenario in 1:num_scenarios
        for block in 1:num_blocks_per_stage[stage]
            data = Quiver.goto!(reader, stage=stage, scenario=scenario, block=block)
            println(data)
        end
    end
end

Quiver.close!(reader)
```

### 2. Converting Between Formats

This example demonstrates how to convert time series data from binary format to CSV. To convert the data in the opposite direction (from CSV to binary), simply switch the positions of `Quiver.binary` and `Quiver.csv` in the function below.

```julia
using Quiver

# Convert binary file to CSV
filename = "path/to/file"
Quiver.convert(filename, Quiver.binary, Quiver.csv)
```

### 3. Merging Multiple Files

This example shows how to merge multiple time series files into a binary single file.

```julia
using Quiver
using Dates

# Define metadata and filenames
filename = "path/to/output/file"
filenames = ["path/to/input_file_1", "path/to/input_file_2", "path/to/input_file_3"]
initial_date = DateTime(2006, 1, 1)
num_stages = 10
num_scenarios = 12
num_blocks = 24
dimensions = ["stage", "scenario", "block"]
time_dimension = "stage"
dimension_size = [num_stages, num_scenarios, num_blocks]

# Merge the files
Quiver.merge(filename, filenames, Quiver.binary)
```

---
