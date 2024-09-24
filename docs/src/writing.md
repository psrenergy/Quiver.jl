## Writing

To write time series data in Quiver, you can leverage different implementations, such as binary and CSV, depending on your performance or readability requirements:

- **CSV Format**: The CSV format is easier to read and inspect manually, as it stores data in a plain-text, tabular format. 

- **Binary Format**: On the other hand, the binary format provides much better performance, especially for large-scale data.

### Writer Structure

The `Writer` structure facilitates writing data efficiently, handling dimensions, labels, and time-related information directly.

#### Writer Fields:

- **filename**: The path where the time series data will be written.
- **dimensions**: An array that specifies the dimensions of the time series (e.g., `["stage", "scenario", "block"]`).
- **labels**: Labels for each time series (e.g., `["agent_1", "agent_2", "agent_3"]`).
- **time_dimension**: The primary time-related dimension, such as "stage".
- **dimension_size**: An array specifying the size of each dimension (e.g., `[num_stages, num_scenarios, num_blocks]`).
- **initial_date**: The starting date of the time series, used for associating data with time.

### Writing to Binary and CSV Formats

Quiver provides two primary implementations for writing time series data:
1. **Binary**: Optimized for performance, allowing efficient storage and retrieval.
2. **CSV**: A human-readable format that can be used when easy inspection or manual editing is needed.

#### Example of writing to binary:

```julia
using Quiver
using Dates

# Define the file path and time series characteristics
filename = "path/to/output/file"
initial_date = DateTime(2024, 1, 1)
num_stages = 10
num_scenarios = 12
num_blocks = 24

# Define dimensions, labels, and time information
dimensions = ["stage", "scenario", "block"]
labels = ["agent_1", "agent_2", "agent_3"]
time_dimension = "stage"
dimension_size = [num_stages, num_scenarios, num_blocks]

# Initialize the Writer for binary format
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
        for block in 1:num_blocks
            data = [stage, scenario, block]  # Example data
            Quiver.write!(writer, data; stage, scenario, block)
        end
    end
end

# Close the writer
Quiver.close!(writer)
```

#### Example of writing to CSV:

```julia
using Quiver
using Dates

# Define the file path and time series characteristics
filename = "path/to/output/file"
initial_date = DateTime(2024, 1, 1)
num_stages = 10
num_scenarios = 12
num_blocks = 24

# Define dimensions, labels, and time information
dimensions = ["stage", "scenario", "block"]
labels = ["agent_1", "agent_2", "agent_3"]
time_dimension = "stage"
dimension_size = [num_stages, num_scenarios, num_blocks]

# Initialize the Writer for CSV format
writer = Quiver.Writer{Quiver.csv}(
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
        for block in 1:num_blocks
            data = [stage, scenario, block]  # Example data
            Quiver.write!(writer, data; stage, scenario, block)
        end
    end
end

# Close the writer
Quiver.close!(writer)
```

### Key Functions:

#### `write!`

This function writes data to the specified dimensions in the file. It validates the dimensions, updates the cache, and writes the provided data.

#### `close!`

This function closes the writer and finalizes the writing process.

```julia
close!(writer)
```