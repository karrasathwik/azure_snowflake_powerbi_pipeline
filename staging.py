import pandas as pd
from connetion import cur

Storage_inti = """CREATE OR REPLACE storage integration azure_of_data
            TYPE = EXTERNAL_STAGE
            ENABLED = TRUE
            STORAGE_PROVIDER = 'AZURE'
            AZURE_TENANT_ID = "deeb1494-3bd3-4d22-abb6-ca4aa46484ec"
            STORAGE_ALLOWED_LOCATIONS = ("azure://snowflakestoragepipe.blob.core.windows.net/snowpipeaz/")
            """
new_con = cur.execute(Storage_inti)
print(new_con)
storage_describe = """DESC INTEGRATION azure_of_data"""
cur.execute(storage_describe)
rows = cur.fetchall()
print(rows)
columns = [desc[2] for desc in cur.description]
df = pd.DataFrame(rows,columns=columns)
print(df)
print(df.columns)

File_type = """ CREATE OR REPLACE FILE FORMAT file_for
                    type = "CSV"
                    FIELD_DELIMITER = ","
                    SKIP_HEADER = 0
                    """
cur.execute(File_type)
print("\n✅ File format 'file_for' created successfully")
#print(file_new)
stage_type = """CREATE OR REPLACE STAGE new_stage
                    storage_integration = azure_of_data
                    file_format = file_for
                    url = ('azure://snowflakestoragepipe.blob.core.windows.net/snowpipeaz/')
                """
cur.execute(stage_type)
print("\n staging is created'stage_type'")
listing = """LIST @new_stage"""
cur.execute(listing)
rows = cur.fetchall()
for row in rows:
    print(row)

raw_data = """SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14 
              FROM @new_stage/'Sample - Superstore.csv' (FILE_FORMAT => 'file_for')"""

cur.execute(raw_data)
da = cur.fetchall()
print(da)