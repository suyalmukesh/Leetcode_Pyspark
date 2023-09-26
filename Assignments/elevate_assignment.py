###  Understanding the Question 
###        Given 2 dataframes Opportunities and Teachers 
###        A teacher would be selected if his availabilty will be met inclusively . A teacher may be available before required day or may be available after last required day .  

### Solution : 
###   This code can be simply executed on any pyspark environment . 
###   The data is created internally , not imported from external sources 


import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("assignmnet").getOrCreate()

#opportunities.csv   
opportunities = [("Maths" , "mon-wed"),
                 ("Hindi" , "tue-sat"),
                 ("CS","thu-fri"),
                 ("Music","mon-fri"),
                 ("Sports","sat-sat")             
               ]
schema= "subjectName string, Day_Range string "
df1 = spark.createDataFrame(data=opportunities,schema=schema)
df1.show()

"""
        +-----------+---------+                                                         
        |subjectName|Day_Range|
        +-----------+---------+
        |      Maths|  mon-wed|
        |      Hindi|  tue-sat|
        |         CS|  thu-fri|
        |      Music|  mon-fri|
        |     Sports|  sat-sat|
        +-----------+---------+
"""

# Teachers
Teachers = [("mukesh" , "Maths" , "mon-fri"),
            ("sunil" , "Sports" , "wed-sat"),
            ("suyal" , "Music" , "tue-fri"),
            ("rohit" , "CS" , "mon-fri")
           ]
schema = "teacherName string ,subject string ,availabilty_range string"
df2 = spark.createDataFrame(data=Teachers,schema=schema)
df2.show()

"""
        +-----------+-------+-----------------+
        |teacherName|subject|availabilty_range|
        +-----------+-------+-----------------+
        |     mukesh|  Maths|          mon-fri|
        |      sunil| Sports|          wed-sat|
        |      suyal|  Music|          tue-fri|
        |      rohit|     CS|          mon-fri|
        +-----------+-------+-----------------+
"""

##  Expression for converting week days into numbers 
##  Why I am doing this - easy to compare   

df1_start_date_num = """case when substr(Day_Range,1,3) == 'mon' then 1
                             when substr(Day_Range,1,3) == 'tue' then 2
                             when substr(Day_Range,1,3) == 'wed' then 3
                             when substr(Day_Range,1,3) == 'thu' then 4
                             when substr(Day_Range,1,3) == 'fri' then 5
                             when substr(Day_Range,1,3) == 'sat' then 6
                        else 0 end """

df1_end_date_num = """case when substr(Day_Range,5,7) == 'mon' then 1
                            when substr(Day_Range,5,7) == 'tue' then 2
                            when substr(Day_Range,5,7) == 'wed' then 3
                            when substr(Day_Range,5,7) == 'thu' then 4
                            when substr(Day_Range,5,7) == 'fri' then 5
                            when substr(Day_Range,5,7) == 'sat' then 6
                     else 0 end """

df2_start_date_num = """case when substr(availabilty_range,1,3) == 'mon' then 1
                             when substr(availabilty_range,1,3) == 'tue' then 2
                             when substr(availabilty_range,1,3) == 'wed' then 3
                             when substr(availabilty_range,1,3) == 'thu' then 4
                             when substr(availabilty_range,1,3) == 'fri' then 5
                             when substr(availabilty_range,1,3) == 'sat' then 6
                       else 0 end """

df2_end_date_num = """case when substr(availabilty_range,5,7) == 'mon' then 1
                            when substr(availabilty_range,5,7) == 'tue' then 2
                            when substr(availabilty_range,5,7) == 'wed' then 3
                            when substr(availabilty_range,5,7) == 'thu' then 4
                            when substr(availabilty_range,5,7) == 'fri' then 5
                            when substr(availabilty_range,5,7) == 'sat' then 6
                    else 0 end """


## Creating start and end columns to both the dataframes 
df1 = df1.withColumn("opportunity_start_date",expr(df1_start_date_num )).withColumn("opportunity_end_date",expr(df1_end_date_num ))
df2 = df2.withColumn("teacher_start_date",expr(df2_start_date_num )).withColumn("teacher_end_date",expr(df2_end_date_num ))

df1.show()
df2.show()

"""
oportunities_df
    +-----------+---------+----------------------+--------------------+
    |subjectName|Day_Range|opportunity_start_date|opportunity_end_date|
    +-----------+---------+----------------------+--------------------+
    |      Maths|  mon-wed|                     1|                   3|
    |      Hindi|  tue-sat|                     2|                   6|
    |         CS|  thu-fri|                     4|                   5|
    |      Music|  mon-fri|                     1|                   5|
    |     Sports|  sat-sat|                     6|                   6|
    +-----------+---------+----------------------+--------------------+

teachers_df
    +-----------+-------+-----------------+------------------+----------------+
    |teacherName|subject|availabilty_range|teacher_start_date|teacher_end_date|
    +-----------+-------+-----------------+------------------+----------------+
    |     mukesh|  Maths|          mon-fri|                 1|               5|
    |      sunil| Sports|          wed-sat|                 3|               6|
    |      suyal|  Music|          tue-fri|                 2|               5|
    |      rohit|     CS|          mon-fri|                 1|               5|
    +-----------+-------+-----------------+------------------+----------------+
"""


## Joining the dataframes based on SUBJECT  

joined_df = df2.join(df1,upper(df1.subjectName) == upper(df2.subject),"inner")

## Note ::  Select Teacher Name with the condition
##              1.  Teacher may be present before the required time 
##              2.  Teacher can also be available after the required time 


joined_df = joined_df.select("teacherName","subject") \
         .where((joined_df["opportunity_start_date"] >= joined_df["teacher_start_date"]) & (joined_df["opportunity_end_date"] <= joined_df["teacher_end_date"])) \
       
joined_df.show()       

"""
+-----------+-------+
|teacherName|subject|
+-----------+-------+
|      rohit|     CS|
|     mukesh|  Maths|
|      sunil| Sports|
+-----------+-------+

"""


####  -------- Completed ---------------- ####