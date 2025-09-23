# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "89ab3a3a-3b30-48ed-b17b-d9829edc4abc",
# META       "default_lakehouse_name": "Dev_CI_CD_Lakehouse",
# META       "default_lakehouse_workspace_id": "45369a67-ed55-4bfc-80f3-dab572c49d66",
# META       "known_lakehouses": [
# META         {
# META           "id": "89ab3a3a-3b30-48ed-b17b-d9829edc4abc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ----------------------------
# 2. Fetch Salesforce data API
# ----------------------------
def fetch_salesforce_data(token: str, offset: int = 0, limit: int = 10):
    base_url = "https://orgfarm-7b6f8c6706-dev-ed.develop.my.salesforce.com"
    query = f"SELECT+FIELDS(ALL)+FROM+Opportunity+LIMIT+{limit}+OFFSET+{offset}"
    api_url = f"{base_url}/services/data/v64.0/query/?q={query}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error {response.status_code}: {response.text}")


# ----------------------------
# 3. Convert API JSON → Spark DF (with offset column)
# ----------------------------
def json_to_spark_df(json_data: dict, spark: SparkSession, offset: int) -> DataFrame:
    records = json_data.get("records", [])
    if not records:
        return None
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    return df.withColumn("offset", lit(offset))


# ----------------------------
# 4. Main ETL Logic
# ----------------------------
spark = SparkSession.builder.getOrCreate()
target_table = "Dev_CI_CD_Lakehouse.dbo.salesforce"
limit = 10

# Step A: Check if table exists
table_exists = spark._jsparkSession.catalog().tableExists(target_table)

if table_exists:
    df_existing = spark.table(target_table)
    # Get latest offset
    last_offset = df_existing.agg(spark_max("offset").alias("max_offset")).collect()[0]["max_offset"]

    if last_offset is not None:
        # Count rows for that offset
        last_offset_count = df_existing.filter(col("offset") == last_offset).count()

        if last_offset_count < limit:
            print(f" Incomplete load detected at OFFSET={last_offset} (rows={last_offset_count}). Deleting and restarting from this offset.")
            # Delete bad rows
            df_existing = df_existing.filter(col("offset") != last_offset)
            df_existing.write.mode("overwrite").saveAsTable(target_table)
            offset = last_offset
        else:
            offset = last_offset + limit
    else:
        offset = 0
else:
    offset = 0

print(f"Starting load from OFFSET={offset}")

# Step B: Load Data from Salesforce
token = get_access_token()
final_df = None

while True:
    print(f"Fetching records with OFFSET={offset}")
    data = fetch_salesforce_data(token, offset, limit)

    df = json_to_spark_df(data, spark, offset)
    if df is None or df.count() == 0:
        print("No more records found. Stopping.")
        break

    # Union all pages into one DataFrame
    if final_df is None:
        final_df = df
    else:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    offset += limit

    # stop if Salesforce says no more records
    if not data.get("done", True) and "nextRecordsUrl" not in data:
        break

# Step C: Save to Lakehouse
if final_df is not None:
    final_df.write.option("mergeSchema", "true").mode("append").saveAsTable(target_table)
    print(f"Data load completed into {target_table}")
else:
    print("No records to write")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
