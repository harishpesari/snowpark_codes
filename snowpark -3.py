# 3. Join Two DataFrames
# Get employee name, department name, and location by joining employees and departments on dept_id.

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

# # Get employee name, department name, and location by joining employees and departments on dept_id.

fin_df = emp_df.join(dep_df,emp_df.col("dept_id")==dep_df.col("dept_id"),join_type="inner")

final_df= fin_df.select(emp_df["emp_name"],dep_df["dept_name"],dep_df["location"])

final_df.show()