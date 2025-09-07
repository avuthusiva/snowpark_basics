from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"dl02145.ap-southeast-1",
"user":"pavan",
"password": "Pradeephc067$",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()


schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/employees01.csv')

# Write data frame employee_s3 to employee table in snowflake.
employee_s3.write.mode("overwrite").save_as_table("employee")

employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/employees02.csv')

employee_s3.write.mode("append").save_as_table("employee")