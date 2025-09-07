
# Execute below commands in snowflake before you start

# create database demo_db;
# use database demo_db;

# create or replace file format my_csv_s3_format
# type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"';


# create or replace stage my_s3_stage  
#   url = 's3://snowflakesmpdata'
#   file_format = my_csv_s3_format;

# create table in snowflake using below command,

# create table int_emp_details_orc(

#   registration_dttm 	timestamp,
#     	id 			     int,
#     	first_name 		string,
#     	last_name 		string,
#     	email 			string,
#     	gender 			string,
#     	ip_address 		string,
#     	cc 			    string,
#     	country 		string,
#     	birthdate 		string,
#     	salary 			double,
#     	title 			string,
#     	comments 		string
# );


from pickle import TRUE
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

# Read data from s3 from the location, '@my_s3_stage/orc_folder/' and create dataframe, employee_s3_orc
employee_s3_orc = session.read.parquet('@my_s3_stage/orc_folder/')

# Mention command to list all the columns in dataframe, employee_s3_orc
employee_s3_orc.columns

# Select only below columns from the dataframe, '"_col1"','"_col2"','"_col3"','"_col4"'
employee_s3_orc = employee_s3_orc.select(['"_col1"','"_col2"','"_col3"','"_col4"'])


# Rename, '"_col1"' to 'id' ; '"_col2"' ---> 'first_name' ; '"_col3"' ---> 'last_name' ; '"_col4"' ---> 'email'
# Write only selected columns to snowflake
employee_s3_orc = employee_s3_orc.with_column_renamed('"_col1"','id'). \
with_column_renamed('"_col2"','first_name'). \
with_column_renamed('"_col3"','last_name').with_column_renamed('"_col4"','email')

employee_s3_orc.write.mode("append").save_as_table("int_emp_details_orc",column_order="name")


