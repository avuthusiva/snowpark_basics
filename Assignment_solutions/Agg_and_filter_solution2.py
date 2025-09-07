
import os
import snowflake.snowpark.functions
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

# /*** #4 Display the TOTAL SALARY drawn by an analyst working in dept no 20 ***/

# SELECT SUM(SALARY+COMMISSION) AS TOTALSALARY FROM EMPLOYEE
# WHERE JOB = 'ANALYST' AND DEPTCODE = 20;

from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_, avg as avg_, min as min_
emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLY")
emp_stg_tbl.where((col("JOB")=='ANALYST') & (col("DEPTCODE")==20)).agg(sum_(col("SALARY")+col("COMMISSION"))).show()

#5 Compute average, minimum and maximum salaries of the group of employees having the job of ANALYST.

# SELECT AVG(Salary) AS AVG_SALARY, MIN(Salary) AS MINSALARY, MAX(Salary) AS MAXSALARY
# FROM EMPLOYEE WHERE Job = 'ANALYST';
emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLY")
sal = emp_stg_tbl.where(col("JOB")=='ANALYST').agg(avg_("SALARY"),max_("SALARY"),min_("SALARY"))
sal.show()