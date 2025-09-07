from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"kp41433.ap-southeast-1",
"user":"pavan",
"password": "Abc123123",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

import pandas as pd

test = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4,5)], columns=["a", "b", "c", "d","e"]))

test.show()

type(test)

test2 = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_GDGL5S36VF")

test2.show()