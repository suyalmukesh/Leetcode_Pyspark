from pyspark.sql import SparkSession
from pyspark.sql.functions import lag,lead,coalesce,when,col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ConsecutiveSeats").getOrCreate()

data = [(1,1),(2,0),(3,1),(4,1),(5,1)]
schema = "seat_id integer , free integer"

df1  = spark.createDataFrame(data=data,schema=schema)
df1.show()

df2 = df1
window_spec1 =  Window.orderBy("seat_id")

#df2 = df1.withColumn("prev",when((lag("seat_id",1).over(window_spec1)).isNull(),col("seat_id")).otherwise(lag("seat_id",1).over(window_spec1)))
df2 = df1.withColumn("prev",lag("free",1).over(window_spec1))
df2 = df2.withColumn("next",lead("free",1).over(window_spec1))
df2.show()
"""
        +-------+----+----+----+
        |seat_id|free|prev|next|
        +-------+----+----+----+
        |      1|   1|null|   0|
        |      2|   0|   1|   1|
        |      3|   1|   0|   1|
        |      4|   1|   1|   1|
        |      5|   1|   1|null|
        +-------+----+----+----+

"""

free_consecutive_seats = df2.select("seat_id").filter("free = 1 and (prev = 1 or next = 1)")
free_consecutive_seats.show()







