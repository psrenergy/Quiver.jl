using Quiver
using Dates
using DataFrames
using Arrow

filename = "test7.arrow"
dimensions = ["stage", "scenario"]
agents = ["agent_$i" for i in 1:500]
writer = Quiver.create_file(filename, dimensions, agents; initial_date = DateTime(2006, 1, 1), stage_type = "month")

dimensions = [collect(Int32, 1:1000) repeat(Int32[1], 1000)] 
@time for _ in 1:2000
    agents = rand(Float32, 1000, 500)
    Quiver.write!(writer, dimensions, agents)
end
Quiver.close!(writer)

df = Quiver.dataframe("test7.arrow")
s = df |> Matrix |> sizeof
s / 1e6

stream = Arrow.Stream(filename)

p = 1
for table in stream
    p += 1
end