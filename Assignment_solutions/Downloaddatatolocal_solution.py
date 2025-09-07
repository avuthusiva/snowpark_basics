from pickle import TRUE
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

# Create a temp stage.
_ = session.sql("create or replace temp stage demo_db.public.mystage").collect()

# Unload data from snowflake table employee to stage locaion @mystage/download/
emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLOYEE")
copy_result = emp_stg_tbl.write.copy_into_location('@mystage/download/', file_format_type="csv", header=True, overwrite=True, single=True)


# Download files from internal stage to your local path
get_result1 = session.file.get("@myStage/download/", "data/downloaded/emp/")