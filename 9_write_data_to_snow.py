# Write data after reading from snow table.

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"ijvunnh-ny22848",
"user":"pradeep",
"password": "AbcdAbcdAbcd067$",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")

customer = customer.filter(col("C_NATIONKEY")=='23').select("C_NAME")

customer.write.mode()

customerwrt = customer.write.mode("append").save_as_table("DEMO_DB.PUBLIC.SNOW_CUSTOMER")

# Write data after reading from s3
session = Session.builder.configs(connection_parameters).create()

# Mention command to read data from '@my_s3_stage/json_folder/'
employee_s3_json = session.read.json('@my_s3_stage/json_folder/')
employee_s3_json = employee_s3_json.select(col("$1").as_("book_tbl"))
employee_s3_json.write.mode("append").save_as_table("DEMO_DB.PUBLIC.JSON_BOOK")


employee_s3_json = employee_s3_json.select_expr("book_tbl:author","book_tbl:id","book_tbl:cat")
employee_s3_json.show()
employee_s3_json.columns
employee_s3_json = employee_s3_json.select(col('"BOOK_TBL:AUTHOR"').as_("author"),col('"BOOK_TBL:ID"').as_("id"),col('"BOOK_TBL:CAT"').as_("cat"))
employee_s3_json.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.JSON_BOOK_PARSED")

# Write csv data from s3 to snowflake.

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

employee_s3_csv = session.read.schema(schema).csv('@my_s3_stage/employee/')
employee_s3_csv.show()
employee_s3_csv = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage/employee/')
employee_s3_csv.write.mode("append").save_as_table("DEMO_DB.PUBLIC.EMPLOYEE_CSV")