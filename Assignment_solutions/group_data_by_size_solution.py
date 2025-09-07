import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

import pandas as pd

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"dl02145.ap-southeast-1",
"user":"pavan",
"password": "*******",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)

session.create_dataframe()

from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

schema = StructType([StructField("school_code", StringType()), StructField("class", StringType()), \
StructField("name", StringType()),  StructField("date_Of_Birth", StringType()), \
    StructField("age", IntegerType()), StructField("height", IntegerType()), \
        StructField("weight", IntegerType()), StructField("address", StringType())])


student_data_snowpark = session.create_dataframe([['s001','V','Alberto Franco','15/05/2002',12,173,35,'street1'],['s002','V','Gino Mcneill','17/05/2002',12,192,32,'street2'],
['s003','VI','Ryan Parkes','16/02/1999',13,186,33,'street3'],['s001','VI','Eesha Hinton','25/09/1998',13,167,30,'street1'],
['s002','V','Gino Mcneill','11/05/2002',14,151,31,'street2'],['s004','VI','David Parkes','15/09/1997',12,159,32,'street4']], schema)

student_data_snowpark.groupBy("school_code").count().show()

