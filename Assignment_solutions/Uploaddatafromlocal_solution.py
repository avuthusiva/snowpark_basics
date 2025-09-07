# Create table by name employee in snowflake

# create or replace TABLE EMPLOYEE (
# 	FIRST_NAME VARCHAR(16777216),
# 	LAST_NAME VARCHAR(16777216),
# 	EMAIL VARCHAR(16777216),
# 	ADDRESS VARCHAR(16777216),
# 	CITY VARCHAR(16777216),
# 	DOJ DATE
# );


## Create below Schema,

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"*****.ap-southeast-1",
"user":"*****",
"password": "********",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()

# Mention command to create temporary stage
#1. Create temporary stage by name mystage
_ = session.sql("create or replace temp stage demo_db.public.mystage").collect()

# Mention the command to upload employee file from local to internal stage.
# 1. Store the result in variable by name, put_result
put_result = session.file.put("data/employee.csv", "@mystage/employee.csv")

# Try to get source_size of the file uploaded
put_result[0].source_size

# Use session.read.schema and session.read.csv 
# 1.Mention the command to read data from internal stage
# 2.Store the returned result in variable by name, employee_local
employee_local = session.read.schema(schema).csv('@mystage')

# Use copy_into_table method,
# 1 Try to copy the data from internal stage to snowflake table, Employee. 
# 2.Store the returned result in dataframe by name copied_into_result
copied_into_result = employee_local.copy_into_table("employee", target_columns=['FIRST_NAME','LAST_NAME','EMAIL','ADDRESS','CITY','DOJ'], force=True,on_error="CONTINUE")