
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


#1 Create a query that displays EMPFNAME, 
# EMPLNAME, DEPTCODE, DEPTNAME, LOCATION
#  from EMPLOYEE, and DEPARTMENT tables. 
# Make sure the results are in the ascending 
# order based on the EMPFNAME and LOCATION of the department.

#STEP 1: Read data from EMPLOYEE and LOCATION to dataframe, emp_stg_tbl and emp_dpt_tbl

emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLY")
emp_dpt_tbl = session.table("DEMO_DB.PUBLIC.DEPARTMENT")

#STEP 2: Join dataframe, emp_stg_tbl and emp_dpt_tbl by column DEPTCODE and create data-frame, emp_dpt_join

emp_dpt_join = emp_stg_tbl.join(emp_dpt_tbl,emp_dpt_tbl.col("DEPTCODE") == emp_stg_tbl.col("DEPTCODE"))
emp_dpt_join = emp_stg_tbl.join(emp_dpt_tbl,emp_dpt_tbl.DEPTCODE == emp_stg_tbl.DEPTCODE)
emp_dpt_join = emp_stg_tbl.join(emp_dpt_tbl,emp_dpt_tbl.DEPTCODE == emp_stg_tbl.DEPTCODE,"Inner")
emp_dpt_join = emp_stg_tbl.join(emp_dpt_tbl,"DEPTCODE","Inner")

#STEP3:  Select only columns, "EMPFNAME","EMPLNAME","DEPTCODE","DEPTNAME","LOCATION"  from data-frame emp_dpt_join and create data-frame by name, emp_dpt_select


emp_dpt_select = emp_dpt_join.select("EMPFNAME","EMPLNAME","DEPTCODE","DEPTNAME","LOCATION")
emp_dpt_select = emp_dpt_join.select(col("EMPFNAME"),col("EMPLNAME"),col("DEPTCODE"),col("DEPTNAME"),col("LOCATION"))
emp_dpt_select = emp_dpt_join.select(["EMPFNAME","EMPLNAME","DEPTCODE","DEPTNAME","LOCATION"])
emp_dpt_select = emp_dpt_join.select(emp_dpt_join.EMPFNAME, emp_dpt_join.EMPLNAME, emp_dpt_join.DEPTCODE, emp_dpt_join.DEPTNAME, emp_dpt_join.LOCATION)
emp_dpt_select = emp_dpt_join.select(emp_dpt_join["EMPFNAME"],emp_dpt_join["EMPLNAME"],emp_dpt_join["DEPTCODE"],emp_dpt_join["DEPTNAME"],emp_dpt_join["LOCATION"])


#STEP4:  Mention command to order column by EMPFNAME in ascending order and create dataframe by name, emp_dpt_order_by

emp_dpt_order_by = emp_dpt_select.order_by("EMPFNAME",ascending=True)
emp_dpt_order_by = emp_dpt_select.order_by(col("EMPFNAME"), col("EMPLNAME").asc())
emp_dpt_order_by = emp_dpt_select.order_by(["EMPFNAME", col("EMPLNAME"),col("DEPTCODE")],ascending=[1,0,0])