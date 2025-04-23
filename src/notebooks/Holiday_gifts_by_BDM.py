# Databricks notebook source
# MAGIC %md
# MAGIC # Raw data file

# COMMAND ----------

file_path='/FileStore/data_files/2024_Holiday_Gifts_List_of_Customers.csv'

# COMMAND ----------

dbfs_path=f'dbfs:{file_path}'
dbutils.fs.ls(dbfs_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # PySpark imports

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Define global variables

# COMMAND ----------

STANDARDIZED_CO_NAME__COLUMN:str = 'normalized_company_name'
NORMALIZED_CO_CODE__COLUMN:str = 'normalized_company_code'

# Using not-capturing group with (?:)
REGEX_PATTERN:str = '((?:\s|\,)+(?:llc|inc|co|company|corp|corporation|group|services|systems)\.?)$'   # regex pattern for llc or llc. would be '(\sllc[\.]?)$'

DBX_TABLE_NAME:str = "normazlize_bdm_company"

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze layer

# COMMAND ----------

raw_df = spark.read.csv(path=dbfs_path,header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver layer

# COMMAND ----------

silver_df = raw_df.select(
    'NAME', 
    trim(col('COMPANY')).alias('Company'), 
    'EMAIL_ID', 
    col('PHONE NUMBER').alias('Phone'), 
    'ADDRESS', 
    'BDM', 
    'IG', 
    'Source'
) \
.withColumn(STANDARDIZED_CO_NAME__COLUMN, lower('Company'))

# to handle case where the Company name is like "Savvas Learning Company LLC"
for idx in range(2):
    temp_column_name = f'{STANDARDIZED_CO_NAME__COLUMN}_{idx}'
    silver_df = silver_df \
        .withColumn(temp_column_name, regexp_replace(col(STANDARDIZED_CO_NAME__COLUMN), lit(REGEX_PATTERN), lit(''))) \
        .drop(STANDARDIZED_CO_NAME__COLUMN) \
        .withColumnRenamed(temp_column_name, STANDARDIZED_CO_NAME__COLUMN)

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Soundex to find similarity

# COMMAND ----------

bdm_and_company_df = silver_df \
    .select('BDM', 'Company') \
    .dropDuplicates() \
    .withColumn(NORMALIZED_CO_CODE__COLUMN, soundex('Company'))

display(bdm_and_company_df.sort('BDM', NORMALIZED_CO_CODE__COLUMN))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windowing to take first occurance

# COMMAND ----------

bdm_and_company_df \
    .dropna() \
    .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
    .saveAsTable(DBX_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ---
# MAGIC --- Using ROW_NUMBER() 
# MAGIC ---
# MAGIC
# MAGIC WITH 
# MAGIC ranked_table AS (
# MAGIC     SELECT
# MAGIC         BDM,
# MAGIC         Company,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY normalized_company_code ORDER BY Company) AS row_num
# MAGIC     FROM
# MAGIC         normazlize_bdm_company
# MAGIC )
# MAGIC ,
# MAGIC dedup_table AS (
# MAGIC   SELECT
# MAGIC       BDM,
# MAGIC       Company
# MAGIC   FROM
# MAGIC       ranked_table
# MAGIC   WHERE
# MAGIC       row_num = 1
# MAGIC   ORDER BY
# MAGIC     BDM, Company
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   BDM,
# MAGIC   concat_ws(', ', collect_list(Company)) as Clients
# MAGIC FROM 
# MAGIC   dedup_table
# MAGIC GROUP BY
# MAGIC   BDM
# MAGIC ORDER BY
# MAGIC   BDM ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ---
# MAGIC --- Using FIRST_VALUE() analytical function
# MAGIC ---
# MAGIC
# MAGIC WITH 
# MAGIC first_value_table AS (
# MAGIC   SELECT
# MAGIC     BDM,
# MAGIC     Company,
# MAGIC     FIRST_VALUE(Company) OVER (PARTITION BY normalized_company_code ORDER BY Company) AS Clients
# MAGIC   FROM
# MAGIC     normazlize_bdm_company
# MAGIC   ORDER BY
# MAGIC     BDM, Company
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   BDM,
# MAGIC   concat_ws(', ', collect_list(Company)) as Clients
# MAGIC FROM 
# MAGIC   first_value_table
# MAGIC GROUP BY
# MAGIC   BDM
# MAGIC ORDER BY
# MAGIC   BDM ASC;
