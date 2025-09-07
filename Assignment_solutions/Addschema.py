import os
from sqlite3 import Date
from xmlrpc.client import DateTime
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

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

schema = StructType([StructField("Name", StringType()), StructField("Salary", IntegerType()), StructField("DOJ",DateType())])

schematypes = session.create_dataframe([["John", "100","2016-01-01"], ["Sam", "200","2017-01-01"]], schema)
schematypes.schema

schematypes.show()