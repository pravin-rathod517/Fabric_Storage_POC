# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

V1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MPH_Variable_Library = notebookutils.variableLibrary.getLibrary("MPH_Variable_Library")
table_path = f"abfss://{MPH_Variable_Library.workspaceid}@onelake.dfs.fabric.microsoft.com/{MPH_Variable_Library.Bronze_Lakehouse_ID}/Tables/{MPH_Variable_Library.NB_Data_Tranfer_Table}"



print(table_path)
df_contact = spark.read.format("delta").load(table_path)
display(df_contact) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filtered = df_contact.filter(
    (df_contact.Gender == "Male")
)

display(df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to save the table (can be same or new table)
output_table_path = f"abfss://{MPH_Variable_Library.workspaceid}@onelake.dfs.fabric.microsoft.com/{MPH_Variable_Library.Bronze_Lakehouse_ID}/Tables/{MPH_Variable_Library.NB_Data_Tranfer_Table}_Filtered/"

# Write the DataFrame as Delta table
df_filtered.write.format("delta").mode("overwrite").save(output_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to save the table (can be same or new table)
output_table_path = f"abfss://{MPH_Variable_Library.workspaceid}@onelake.dfs.fabric.microsoft.com/{MPH_Variable_Library.Bronze_Lakehouse_ID}/Tables/{MPH_Variable_Library.NB_Data_Tranfer_Table}_Filtered/"

# Write the DataFrame as Delta table
df_filtered.write.format("delta").mode("overwrite").save(output_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
