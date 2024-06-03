import math
from math import ceil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, sum, when, ceil as spark_ceil, explode
from pyspark.sql.functions import monotonically_increasing_id, row_number, first, avg
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from collections import defaultdict

# Initialize Spark session
spark = SparkSession.builder.appName("CommScore").getOrCreate()

# Load the final DataFrame from HDFS
final_df = spark.read.json("hdfs:///population_density.json")

# Load bus stops data from HDFS
bus_stops_raw_df = spark.read.json("hdfs:///buses/stops/bus_stops")

bus_stops_df = bus_stops_raw_df.select(explode(col("values")).alias("values")) \
    .selectExpr("values.key as key", "values.value as value") \
    .where("key == 'dlug_geo' or key == 'szer_geo'")

df = bus_stops_df.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())))

# Create grouping identifier for each pair
df = df.withColumn("group_id", ((col("id") - 1) / 2).cast("integer"))

# Pivot the DataFrame
pivoted_df = df.groupBy("group_id").pivot("key").agg(first("value"))

# Drop the grouping identifier if not needed
pivoted_df = pivoted_df.drop("group_id")


bus_stops_df = pivoted_df.selectExpr("dlug_geo as longitude", "szer_geo as latitude")

# Define the geographic bounds and cell size
left = 20.852142
right = 21.257111
top = 52.366347
bottom = 52.100750
cell_size = 0.02  # Adjust the cell size as needed

# Function to assign points to grid cells
def assign_to_grid(df, left, bottom, cell_size):
    df = df.withColumn(
        "x",
        ((col("longitude") - left) / cell_size).cast(IntegerType())
    ).withColumn(
        "y",
        ((col("latitude") - bottom) / cell_size).cast(IntegerType())
    )
    return df

final_df = assign_to_grid(final_df, left, bottom, cell_size)
bus_stops_df = assign_to_grid(bus_stops_df, left, bottom, cell_size)

# Broadcast the bus stops DataFrame
bus_stops_broadcast = broadcast(bus_stops_df)

# Function to get adjacent cells
def get_adjacent_cells(x, y):
    return [(x+i, y+j) for i in range(-1, 2) for j in range(-1, 2)]

# Register the haversine function as a UDF
def haversine(lon1, lat1, lon2, lat2):
    R = 6371  # Radius of Earth in kilometers
    lon1 = float(lon1)
    lon2 = float(lon2)
    lat1 = float(lat1)
    lat2 = float(lat2)
    dlon = math.radians(lon2 - lon1)
    dlat = math.radians(lat1 - lat2)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance * 1000

haversine_udf = spark.udf.register("haversine_udf", haversine, DoubleType())

# Explode final_df to create pairs of each final point with its adjacent cells
adj_cells = final_df.select("x", "y").distinct().rdd.flatMap(lambda row: get_adjacent_cells(row.x, row.y)).distinct().toDF(["adj_x", "adj_y"])

# Join final points with their corresponding adjacent cells
final_adj_df = final_df.join(adj_cells, (final_df.x == adj_cells.adj_x) & (final_df.y == adj_cells.adj_y)).select(
    final_df["*"],
    adj_cells["adj_x"],
    adj_cells["adj_y"]
)

# Join with bus stops on adjacent cells
final_bus_stops_df = final_adj_df.join(bus_stops_broadcast, (final_adj_df.adj_x == bus_stops_broadcast.x) & (final_adj_df.adj_y == bus_stops_broadcast.y)).select(
    final_adj_df["population"],
    final_adj_df["longitude"],
    final_adj_df["latitude"],
    final_adj_df["x"],
    final_adj_df["y"],
    bus_stops_broadcast["longitude"].alias("bus_stop_longitude"),
    bus_stops_broadcast["latitude"].alias("bus_stop_latitude")
)

# Calculate the distance for each pair
final_bus_stops_df = final_bus_stops_df.withColumn(
    "distance",
    haversine_udf(col("longitude"), col("latitude"), col("bus_stop_longitude"), col("bus_stop_latitude"))
)

# Aggregate distances and calculate scores
result_df = final_bus_stops_df.groupBy("population", "longitude", "latitude").agg(
    sum(when(col("distance") < 100, 1).otherwise(0)).alias("0_100_stops"),
    sum(when((col("distance") >= 100) & (col("distance") < 1000), 1).otherwise(0)).alias("100_1000_stops"),
    sum(when((col("distance") >= 1000) & (col("distance") < 2000), 1).otherwise(0)).alias("1000_2000_stops"),
)

# Calculate the score based on the stops in different distance ranges
result_df = result_df.withColumn(
    "score",
    spark_ceil((col("0_100_stops") * 10 + col("100_1000_stops") * 5 + col("1000_2000_stops")) / col("population"))
)

window_spec = Window.partitionBy("population", "longitude", "latitude").orderBy("distance")

# Add a row number to each distance within the partition to select the top 3 distances
ranked_distances_df = final_bus_stops_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the top 3 distances
top_3_distances_df = ranked_distances_df.filter(col("rank") <= 3)

# Calculate the average of the top 3 distances
avg_distances_df = top_3_distances_df.groupBy("population", "longitude", "latitude").agg(
    avg("distance").alias("avg_3_distances")
)

# Join the average distances back to the result dataframe
result_df = result_df.join(avg_distances_df, on=["population", "longitude", "latitude"], how="left")

# Select the final columns to display
result_df = result_df.select(
    col("population"),
    col("longitude"),
    col("latitude"),
    col("0_100_stops"),
    col("100_1000_stops"),
    col("1000_2000_stops"),
    col("score"),
    col("avg_3_distances")
)

result_df.show()
# Show the results
result_df.coalesce(1).write.mode("overwrite").json("hdfs:///score.json")

# Stop the Spark session
spark.stop()

