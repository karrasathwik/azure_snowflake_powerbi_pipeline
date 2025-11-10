import pandas as pd
from connetion import cur

Storage_inti = """CREATE OR REPLACE storage integration azure_of_data
            TYPE = EXTERNAL_STAGE
            ENABLED = TRUE
            STORAGE_PROVIDER = 'AZURE'
            AZURE_TENANT_ID = "deeb1494-3bd3-4d22-abb6-ca4aa46484ec"
            STORAGE_ALLOWED_LOCATIONS = ("azure://snowflakestoragepipe.blob.core.windows.net/snowpipeaz/")
            """
cur.execute(Storage_inti)
storage_describe = """DESC INTEGRATION azure_of_data"""
new_cur = cur.execute(storage_describe)