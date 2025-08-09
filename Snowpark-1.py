
# Get the names of all female employees (GENDER = 'F') from the employees table.


from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


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

# Quick check – list all tables
emp_df = session.table("employees")
emp_df.show()
fil_df= emp_df.filter(col("gender")=='F')
fil_df.show()



