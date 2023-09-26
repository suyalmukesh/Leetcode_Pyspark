## Leetcode SQL question 

from pyspark.sql import SparkSession 
from pyspark.sql.functions import to_date,expr
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AppleOranges").getOrCreate()

## Get Data 
input_data1 = [ 
                ( '2020-05-01','apples',10),
                ( '2020-05-01','oranges',8),
                ( '2020-05-02','apples',15),
                ( '2020-05-02','oranges',15),
                ( '2020-05-03','apples',20),
                ( '2020-05-03','oranges',0),
                ( '2020-05-04','apples',15),
                ( '2020-05-04','oranges',16)  
                ]
schema = "sale_date string , fruit string , sold_num integer"

## Create Data Frame 
df1 = spark.createDataFrame(data=input_data1,schema=schema)

## Convert Date from string to Date type 
df1 = df1.withColumn("sale_date",to_date(df1["sale_date"],"yyyy-MM-dd"))

## Case Logic for identifying apple or orange count 
apple   = "case when fruit = 'apples' then sold_num else 0 end "
orange  = "case when fruit = 'oranges' then sold_num else 0 end "


df1 = df1.withColumn("apple",expr(apple))
df1 = df1.withColumn("oranges",expr(orange))


grouped_data = df1.groupBy("sale_date").agg(sum("apple").alias("apple_sum"),sum("oranges").alias("orange_sum"))

grouped_data = grouped_data.withColumn("Difference",expr("apple_sum - orange_sum"))

## Print the result 
print("Given data set ")
df1.show()
print("Final Result :: ")
grouped_data.select("sale_date","Difference").show()
