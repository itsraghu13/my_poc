# Databricks notebook source
from pyspark.sql.functions import input_file_name

# COMMAND ----------

# dbutils.widgets.removeAll()  # (used for to cleanup widgets)

# COMMAND ----------

# Set Default Parameter values
dbutils.widgets.text("sourceFilePath", "/vantage/MemberDemographics/", "Source File Path")
dbutils.widgets.text("sourceFileType", "csv", "Source File Type")
dbutils.widgets.text("sourceFileDelimiter", ",", "File Delimiter")
dbutils.widgets.text("sourceContainerName", "raw", "Source Container Name")
dbutils.widgets.text("sourceSchemaLocation", "source_schema/", "Source Schema Location")
dbutils.widgets.text("sourceFileWildCard", "*", "Source Wild Card")
dbutils.widgets.text("sourceFileContainsHeader", "True", "Source File Contains Header")
dbutils.widgets.text("targetDBName", "structured", "Target Database Name")
dbutils.widgets.text("targetTableName", "MemberDemographics", "Target Table Name")
dbutils.widgets.text("targetContainerName", "structured", "Target Container Name")
dbutils.widgets.text("dfsPath", "devuscedexadb02.dfs.core.windows.net/", "ABFSS Path")

dbutils.widgets.text("autoloaderFileType", "text", "Autoloader File Type")
dbutils.widgets.text("overwriteSchema", "false", "Set to true if target schema should be overwritten with schema from data frame")
dbutils.widgets.text("mergeSchema", "false", "Set to true if target and table schema should be merged (keeping columns from both schemas)")
dbutils.widgets.text("delimiterInferSchema", "false", "Set to true if schema should be inferred")
dbutils.widgets.text("truncateData", "false", "Set to true if data needs to be truncated and loaded")

# incremental load parameters
dbutils.widgets.text("executionTimeout", "43200", "Execution Timeout (Seconds)")
dbutils.widgets.text("mergeData", "false", "Set to true if merge not required")
dbutils.widgets.text("mergeColumns", "employee_id,cost_center_id,manager_id", "Columns used for merge")
dbutils.widgets.text("containsOperationCode", "true", "Set to True if file data contains operation code")
dbutils.widgets.text("operationCodeColumn", "employee_id", "Column name for the column that contains Operation Code Values")
dbutils.widgets.text("operationCodes", "INSERT,UPDATE,DELETE,D", "List of Operations, Operation Codes")

# COMMAND ----------

# Define variables
# fetch from parameters
tbl_name               = dbutils.widgets.get("targetTableName")
db_name                = dbutils.widgets.get("targetDBName")
autoloader_file_type   = dbutils.widgets.get("autoloaderFileType")
actual_file_type       = dbutils.widgets.get("sourceFileType")
delimiter              = dbutils.widgets.get("sourceFileDelimiter")
file_path              = dbutils.widgets.get("sourceFilePath")
file_name_wildcard     = dbutils.widgets.get("sourceFileWildCard")
has_header             = dbutils.widgets.get("sourceFileContainsHeader")
source_zone            = dbutils.widgets.get("sourceContainerName")
target_zone            = dbutils.widgets.get("targetContainerName")
dfs_path               = dbutils.widgets.get("dfsPath")
file_schema_folder     = dbutils.widgets.get("sourceSchemaLocation")
overwrite_schema       = dbutils.widgets.get("overwriteSchema")
merge_schema           = dbutils.widgets.get("mergeSchema")
delimiter_inferSchema  = dbutils.widgets.get("delimiterInferSchema")
truncate_data          = dbutils.widgets.get("truncateData")

# incremental load parameters
execution_timeout      = int(dbutils.widgets.get("executionTimeout"))
merge_data             = dbutils.widgets.get("mergeData")
merge_columns          = dbutils.widgets.get("mergeColumns")
contains_operation_code= dbutils.widgets.get("containsOperationCode")
operation_code_column  = dbutils.widgets.get("operationCodeColumn")
operation_codes        = dbutils.widgets.get("operationCodes")

# assign default values for parameters
autoloader_schema_location     = "auto_loader_schemas"
autoloader_checkpoint_location = "auto_loader_checkpoints"

can_evolve   = "false"
abfss_path   = "abfss://"
excel_type   = "com.crealytics.spark.excel"
valid_autoloader_file_types = ["binaryFile", "csv", "text", "parquet", "json", "avro", "ORC"]

raw_path        = abfss_path + source_zone + "@" + dfs_path + "/" + file_path + file_name_wildcard
db_tbl_path     = abfss_path + target_zone + "@" + dfs_path + "/" + "{}/{}".format(db_name, tbl_name)
al_schema_path  = abfss_path + source_zone + "@" + dfs_path + "/" + autoloader_schema_location + "/{}/{}".format(db_name, tbl_name)
al_chkpt_path   = abfss_path + source_zone + "@" + dfs_path + "/" + autoloader_checkpoint_location + "/{}/{}".format(db_name, tbl_name)
file_schema_path= abfss_path + source_zone + "@" + dfs_path + "/" + file_schema_folder + "/"

print(al_schema_path)

# COMMAND ----------

# Perform Delimited Input Validations
print(al_schema_path)

# COMMAND ----------

# Read Stream
df_text = (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", autoloader_file_type)
      .option("header", has_header)
      .option("cloudFiles.schemaLocation", al_schema_path)
      .option("checkpointLocation", al_chkpt_path)
      .option("delimiter", delimiter)
      .option("inferSchema", delimiter_inferSchema)
      .load(raw_path)
      .selectExpr("*", "_metadata.file_path as file_name")
)

# COMMAND ----------

from time import sleep
sleep(5)

# COMMAND ----------

# Rename columns containing spaces  (FIXED OVERLAP - DO NOT CHANGE LOGIC)
from pyspark.sql import functions as F

renamed_df = df_text.select(
    [F.col(col).alias(col.replace(" ", "_").replace(".", "_")) for col in df_text.columns]
)

# COMMAND ----------

# Truncate Table if truncate_data
if truncate_data == "True" and spark.catalog.tableExists(db_name + "." + tbl_name):
    sql_query = "TRUNCATE TABLE " + db_name + "." + tbl_name
    results = spark.sql(sql_query)

# COMMAND ----------

# if merge_data is true, create a new staging table with _cdc and drop the table if it already exists
if merge_data == "True":
    target_tbl_name = tbl_name
    tbl_name = tbl_name + "_cdc_merge"

    # drop the target table if it exists
    if spark.catalog.tableExists(db_name + "." + tbl_name):
        sql_query = "DROP TABLE " + db_name + "." + tbl_name
        results = spark.sql(sql_query)

# COMMAND ----------

# Write Stream
(
    renamed_df.writeStream
      .format("delta")
      .option("checkpointLocation", al_chkpt_path)
      .option("overwriteSchema", overwrite_schema)
      .option("mergeSchema", merge_schema)
      .trigger(once=True)
      .table(db_name + "." + tbl_name)
)

# COMMAND ----------

from time import sleep
sleep(5)

# COMMAND ----------

# call merge notebook and pass the required parameters
if merge_data == "True":
    dbutils.notebook.run(
        "./Merge_Incremental_Load",
        execution_timeout,
        {
            "source_db_name": db_name,
            "source_table_name": tbl_name,
            "target_db_name": db_name,
            "target_table_name": target_tbl_name,
            "mergeData": merge_data,
            "mergeColumns": merge_columns,
            "containsOperationCode": contains_operation_code,
            "operationCodeColumn": operation_code_column,
            "operationCodes": operation_codes
        }
    )

# COMMAND ----------

# Verify Results
# (keep your existing verification cells below if you have them)
