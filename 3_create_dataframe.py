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
session.sql("use warehouse compute_wh").collect()

test = session.create_dataframe({1, 2, 3, 4}, schema=["a"])

# create a dataframe by inferring a schema from the data

test = session.create_dataframe([1, 2, 3, 4], schema=["a"])
test.show()
type(test)

test = session.create_dataframe([[1, 2, 3, 123],[1, 2, 3, "ABC"],[1, 2, 3, "HPC"],[1, 2, 3, "EMD"]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022']], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, 26.897],[1, 2, 3, 27.897],[1, 2, 3, 29.897],[1, 2, 3, 39.897]], schema=["a","b","c","d"])
test.show(1)

test = session.create_dataframe([[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, {"a":"hi"}],[1, 2, 3, None],[1, 2, 3, {"a":"Bye"}],[1, 2, 3, {"a":"hello"}]], schema=["a","b","c","d"])
test.show()

test = session.create_dataframe([[1, 2, 3, ["Hi"]],[1, 2, 3, None],[1, 2, 3,["Hello"] ],[1, 2, 3, ["Namaste"]]], schema=["a","b","c","d"])

test1 = test.cache_result()
test1.show()
type(test1)

# Check performance

begin = time.time()
test.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")


begin = time.time()
test1.show()
end = time.time()
print(f"Total runtime of the program is {end - begin}")


