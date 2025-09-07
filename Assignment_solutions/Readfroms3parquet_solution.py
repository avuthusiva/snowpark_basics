
# Execute below commands in snowflake before you start

# create database demo_db;
# use database demo_db;

# create or replace file format my_csv_s3_format
# type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true FIELD_OPTIONALLY_ENCLOSED_BY='"';


# create or replace stage my_s3_stage  
#   url = 's3://snowflakesmpdata'
#   file_format = my_csv_s3_format;

# create table in snowflake using below command,

# create table int_emp_details_parquet(

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

# Read data from s3 from the location, '@my_s3_stage/parquet_folder/' and create dataframe, employee_s3_parquet
employee_s3_parquet = session.read.parquet('@my_s3_stage/parquet_folder/')
employee_s3_parquet.show()

# Mention command to list all the columns in dataframe, employee_s3_parquet
employee_s3_parquet.columns

# Select only below columns from the dataframe, first_name , last_name, email, gender
employee_s3_parquet = employee_s3_parquet.select(col('"first_name"'),col('"last_name"'),col('"email"'),col('"gender"'))


# Mention command to write dataframe, employee_s3_parquet to snowflake.
employee_s3_parquet.write.mode("append").save_as_table("int_emp_details_parquet",column_order="name")

# Consider below schema 

schema = StructType([StructField("registration_dttm", TimestampType()),
StructField("id", IntegerType()),
StructField("first_name", StringType()),
StructField("last_name", StringType()),
StructField("email", StringType()),
StructField("gender",StringType()),
StructField("ip_address",StringType()),
StructField("cc",StringType()),
StructField("birthdate",StringType()),
StructField("salary",DoubleType()),
StructField("title",StringType()),
StructField("comments",StringType())])

# Try to apply this schema to employee_s3_parquet dataframe and read data from s3. Mention your observation below
employee_s3_parquet = session.read.parquet('@my_s3_stage/parquet_folder/').schema(schema)