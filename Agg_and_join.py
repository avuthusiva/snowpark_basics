from snowflake.snowpark import Session
from snowflake.snowpark import Window
from snowflake.snowpark.functions import col,min as _min,rank,max,avg
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

connection_parameters = {"account":"kp41433.ap-southeast-1",
"user":"pavan",
"password": "Abc123123",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

session.sql_simplifier_enabled=True

employee = session.table("demo_db.public.employee")
employee_grp = employee.select("FIRST_NAME").group_by(["FIRST_NAME"])
type(employee_grp)
employee_grp_cnt = employee_grp.count()
employee_dups = employee_grp_cnt.select("FIRST_NAME","COUNT").where(col("COUNT")>1)
employee_dups.show()


employee_grp = employee.select("FIRST_NAME").group_by(["FIRST_NAME"]).count().where(col("COUNT")>1)
employee_grp.show()


###################################################################################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")
sup_min_acctbal = supplier.select(_min("s_acctbal"))
sup_min_acctbal.show()
sup_min_acctbal = supplier.select(_min("s_acctbal").as_("min_s_acctbal")).collect()
d = sup_min_acctbal[0]["MIN_S_ACCTBAL"]

sup_min_acctbal["MIN_S_ACCTBAL"]

sup_min_acctbal.columns


supplier.select("s_name","s_acctbal",lit('99.6')).where(col("s_acctbal")=='-998.22').show()

supplier.select("s_name","s_acctbal").filter(col("s_acctbal")==sup_min_acctbal.select("MIN_S_ACCTBAL")).show()

supplier.filter(supplier["s_suppkey"].in_(supplier_filter.select("s_suppkey"))).show()

supplier.select("s_name","s_acctbal").filter(supplier["s_acctbal"].in_(sup_min_acctbal.select("MIN_S_ACCTBAL"))).show()

sup_min_acctbal = supplier.select("s_name","s_acctbal",rank().over(Window.order_by("s_acctbal")).as_("rank")).where(col("RANK")==1)
sup_min_acctbal.show()



###############################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

supplier_agg = supplier.select(min("s_acctbal"),max("s_acctbal"),avg("s_acctbal"))
supplier_agg.show()

agg = [('s_acctbal','min'),('s_acctbal','max'),('s_acctbal','avg')]

supplier_agg = supplier.agg(agg)



###################################################

## Read table 

customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.customer")
orders = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.orders")
lineitem = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.lineitem") 

cust_orders = customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner")
cust_orders_line = cust_orders.join(lineitem,lineitem["l_orderkey"]== cust_orders["o_orderkey"],"inner")
cust_orders_line = cust_orders_line.select("c_custkey","c_name")

cust_orders_line.show()

customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner").\
    join(lineitem,lineitem["l_orderkey"]== orders["o_orderkey"],"inner")\
        .select("c_custkey","c_name").show()


## Joins

cust_orders = customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner")
cust_orders_line = cust_orders.join(lineitem,lineitem["l_orderkey"]==cust_orders["o_custkey"],"inner")
cust_orders_line.select("c_custkey","c_name").show()

customer.join(orders,customer["c_custkey"]==orders["o_custkey"],"inner").\
    join(lineitem,lineitem["l_orderkey"]==orders["o_custkey"],"inner").\
        select("c_custkey","c_name").show()


#######################################################################################

supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")






supplier_filter = supplier.select("s_suppkey","s_nationkey").filter(col("s_nationkey")==1)
supplier.filter(supplier["s_suppkey"].in_(supplier_filter.select("s_suppkey"))).show()