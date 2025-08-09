from snowflake.snowpark import Session
from snowflake.snowpark.functions import *

# Define connection parameters directly
connection_parameters = {
    "account": "ISCJGIV-TX84484",
    "user": "INDRANI",
    "password": "LuchiBuchi@2610", 
    "role": "ACCOUNTADMIN",
    "warehouse": "SNOWFLAKE_LEARNING_WH",
    "database": "SNOWFLAKE_LEARNING_DB",
    "schema": "PUBLIC"
}

# Create Snowpark session
session = Session.builder.configs(connection_parameters).create()

# Load the employees table
emp_df = session.table("EMPLOYEES")
dep_df = session.table("DEPARTMENTS")  

# Find the average salary in each department. Display dept_name and avg_salary.

fin_df = emp_df.join(dep_df,emp_df.col("dept_id")==dep_df.col("dept_id"),join_type="inner")

final_df= fin_df.select(dep_df["dept_name"],emp_df["salary"])

agg_df= final_df.group_by (final_df.col("dept_name")).agg(round(avg(final_df.col("salary")),2).alias("avg_salary"))


agg_df.show()