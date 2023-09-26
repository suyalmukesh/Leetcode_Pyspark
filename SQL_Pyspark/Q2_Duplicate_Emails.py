from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

data = [(1,'a@b.com'),(2,'c@d.com'),(3,'a@b.com')]
schema = "id integer , email string"

spark = SparkSession.builder.appName("Duplicate_Emails").getOrCreate()

df = spark.createDataFrame(data = data,schema=schema)

window_spec = Window.partitionBy("email").orderBy("id")

df = df.withColumn("R",row_number().over(window_spec))

df.select("email").filter("R > 1").show()

"""
INPUT :
+---+-------+                                                                   
| id|  email|
+---+-------+
|  1|a@b.com|
|  2|c@d.com|
|  3|a@b.com|
+---+-------+

OUTPUT: 
+-------+
|  email|
+-------+
|a@b.com|
+-------+

"""