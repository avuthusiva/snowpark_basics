from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {"account":"********",
"user":"*****",
"password": "*******",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())

# /**** #3 Display MAX and 2nd MAX SALARY from the EMPLOYEE table. ****/

# SELECT
# (SELECT MAX(SALARY) FROM EMPLOYEE) MAXSALARY,
# (SELECT MAX(SALARY) FROM EMPLOYEE
# WHERE SALARY NOT IN (SELECT MAX(SALARY) FROM EMPLOYEE )) as ND_MAX_SALARY;
from snowflake.snowpark.types import StringType, IntegerType
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_


emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLY")
first_max_salary = emp_stg_tbl.select(max_("SALARY"))
second_max_salary = emp_stg_tbl.select("SALARY").where(~emp_stg_tbl["SALARY"].in_(first_max_salary.collect()[0][0])).agg(max_("SALARY"))
first_and_second = second_max_salary.union(first_max_salary)