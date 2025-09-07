from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"********",
"user":"*****",
"password": "*******",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

employee_s3 = session.read.csv('@my_s3_stage/employee/')

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/')
employee_s3.show()
employee_s3 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage/employee/')
employee_s3.show()
type(employee_s3)

employee_s3 = employee_s3.cache_result()
employee_s3.is_cached

employee_s4=employee_s3.cache_result()

type(employee_s5)

employee_s3.columns

employee_s5=employee_s3.select("FIRST_NAME","LAST_NAME").filter(col("FIRST_NAME")=='Nyssa')
employee_s5.show()

employee_s3.show()

employee_s3.queries