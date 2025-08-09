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

# Get employees who joined after 1st Jan 2021
fil_df = emp_df.filter(col("DOJ") > to_date(lit("2021-01-01")))
fil_df.show()