import pandas as pd
import snowflake.connector

conn = snowflake.connector.connect(
        user = "jar",
        password = "Sage@sathwik123",
        account = "TWQRAGF-AW62002",
        warehouse = "COMPUTE_WH",
        database = "snowflake_database",
        schema='PUBLIC'
)
print("connection has made")
cur = conn.cursor()

cur.execute("show databases")
result = cur.fetchall()
for row in result:
    print(row)
    
save = cur.execute("USE DATABASE snowflake_database")
print("using",save)


Storage_inti = """CREATE OR REPLACE storage integration azure_of_data
            TYPE = EXTERNAL_STAGE
            ENABLED = TRUE
            STORAGE_PROVIDER = 'AZURE'
            AZURE_TENANT_ID = "deeb1494-3bd3-4d22-abb6-ca4aa46484ec"
            STORAGE_ALLOWED_LOCATIONS = ("azure://snowflakestoragepipe.blob.core.windows.net/snowpipeaz/")
            """
cur.execute(Storage_inti)