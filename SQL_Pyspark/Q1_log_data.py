"""
Question 12 : Suppose you have a table called log_data with the following columns: 
log_id, user_id, action, and timestamp. 
Write a SQL query to calculate the number of actions performed by each user in the last 7 days.
""" 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import to_timestamp,datediff,current_date,date_format,col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Q1").getOrCreate()
data =  [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout','2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
schema = "log_id integer , user_id integer ,action string , timestamp string"


df1 = spark.createDataFrame(data = data , schema = schema)
df1 = df1.withColumn("timestamp",to_timestamp("timestamp","yyyy-MM-dd HH:mm:ss"))
df1 = df1.withColumn("timestamp_date",date_format("timestamp","yyyy-MM-dd"))
df1 = df1.withColumn("current_date",current_date())
df1.show()


df1.select("user_id") \
   .filter(datediff(col("current_date"),col("timestamp_date")) >= 0 ) \
   .groupBy("user_id").count() \
   .withColumnRenamed("count","actions_count") \
   .show()



