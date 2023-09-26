from pyspark.sql import SparkSession
from pyspark.sql.functions import lag,dense_rank
from pyspark.sql.window import Window 


employee = [(1,'Joe',70000,1),
            (2,'Jim',90000,1),
            (3,'Henry',80000,2),
            (4,'Sam',60000,2),
            (5,'Max',90000,1)
           ]

department = [(1,'IT'),
              (2,'Sales')
            ]


spark = SparkSession.builder.appName("DepartmentHighestSalary").getOrCreate()
df_employee = spark.createDataFrame(data=employee,schema="id integer,name string,salary integer,departmentId integer")
df_department = spark.createDataFrame(data=department,schema="id integer,name string")

df_employee = df_employee.withColumnRenamed("id","emp_id")
df_employee = df_employee.withColumnRenamed("name","emp_name")
df_department = df_department.withColumnRenamed("id","departmentId")
df_department = df_department.withColumnRenamed("name","dep_name")
df_employee.show()
df_department.show()

## Main Logic

window_spec = Window.partitionBy("departmentId").orderBy(df_employee["salary"].desc())

#df_group = df_employee.select("departmentId","name","salary",dense_rank().over(window_spec))

df_group = df_employee
df_group = df_group.withColumn("R",dense_rank().over(window_spec))

df_group = df_group.withColumnRenamed("departmentId","depId")
df_group.show()

df_join = df_department.join(df_group,df_department["departmentId"] == df_group["depId"],"inner" )

df_join.show()

df_result = df_join.select(df_join["dep_name"].alias("department") \
               ,df_join["emp_name"].alias("employee")\
               ,df_join["salary"]) \
               .filter("R = 1") \
               .orderBy("salary")

df_result.show()