# Execute below commands in snowflake before you start

# create database demo_db;
# use database demo_db;

# create or replace file format my_csv_s3_format
# type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"';


# create or replace stage my_s3_stage  
#   url = 's3://snowflakesmpdata'
#   file_format = my_csv_s3_format;

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

# Mention command to read data from '@my_s3_stage/json_folder/'
employee_s3_json = session.read.json('@my_s3_stage/json_folder/')
employee_s3_json.show()

# Mention command to select , author, cat , genre_s, id from dataframe, employee_s3_json
employee_s3_json = employee_s3_json.select_expr("$1:author","$1:cat","$1:genre_s","$1:id")

employee_s3_json.schema

employee_s3_json2 = employee_s3_json.with_column_renamed('"$1:AUTHOR"','author').\
    with_column_renamed('"$1:CAT"','cat').\
        with_column_renamed('"$1:GENRE_S"','genre_s').\
            with_column_renamed('"$1:ID"','id')

employee_s3_json2.show()

# Write dataframe, employee_s3_json to snowflake table.
employee_s3_json2.write.mode("append").save_as_table("author_details_json",column_order="name")