import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {"account":"kp41433.ap-southeast-1",
"user":"pavan",
"password": "Abc123123",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

test_session = Session.builder.configs(connection_parameters).create()

print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

session = Session.builder.configs(connection_parameters).create()