# Databricks notebook source
import os 

volume = "/Volumes/dataentrepreneurssynthetic/dataentrepreneurssynthetic/dataentrepreneurssynthetic"

files = os.listdir(volume)
print(files)

# COMMAND ----------

Beneficiary = spark.read.csv(f"{volume}/Beneficiary_Summary_File.csv", header=True, inferSchema=True)

display(Beneficiary)


# COMMAND ----------

Inpatient = spark.read.csv(f"{volume}/Inpatient_Claims.csv",header=True, inferSchema=True)

display(Inpatient)

# COMMAND ----------

Outpatient = spark.read.csv(f"{volume}/Outpatient_Claims.csv", header=True, inferSchema=True)

display(Outpatient)

# COMMAND ----------

from pyspark.sql.functions import col 

Outpatient = Outpatient.withColumn("date_missing", col("CLM_FROM_DT").isNull())

display(Outpatient)

# COMMAND ----------

from pyspark.sql.functions import to_date, col 

Beneficiary= Beneficiary.withColumn(
    "birth_date_clean", to_date(col("BENE_BIRTH_DT").cast("string"),"yyyyMMdd" ))

Beneficiary.select("BENE_BIRTH_DT","birth_date_clean").display()
    

# COMMAND ----------

Beneficiary.write.mode("overwrite").saveAsTable("Beneficiary_table")


# COMMAND ----------


Outpatient = Outpatient.withColumn("date_missing", col("CLM_FROM_DT").isNull())
Outpatient = Outpatient.withColumn("clean_date", to_date(col("CLM_FROM_DT").cast("string"),"yyyyMMdd"))

Outpatient.select("date_missing","CLM_FROM_DT","clean_date").display()

# COMMAND ----------

Outpatient.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("Outpatient_table")

# COMMAND ----------

Inpatient.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("Inpatient_table")

# COMMAND ----------

from pyspark.sql.functions import when, col 

Beneficiary = Beneficiary.withColumn(
    "sex_clean", 
     when(col("BENE_SEX_IDENT_CD") == 1, "Male")
    .when(col("BENE_SEX_IDENT_CD") == 2, "Female")
    .otherwise("Unknown"))

Beneficiary.select("BENE_SEX_IDENT_CD","sex_clean").display()

# COMMAND ----------

from pyspark.sql.functions import col, when

joined = Inpatient.join(Beneficiary, on="DESYNPUF_ID", how="inner")

joined = joined.withColumnRenamed("CLM_PMT_AMT", "Cost")
joined = joined.withColumnRenamed("SP_ALZHDMTA", "Dementia")
joined = joined.withColumnRenamed("DESYNPUF_ID", "ID")

joined = joined.withColumn(
    "has_alzheimers",
    when(col("Dementia") == 1, "Yes")
    .when(col("Dementia") == 2, "No")
    .otherwise("Unknown")
)

joined.select("ID", "Cost", "has_alzheimers").display()

# COMMAND ----------

summary = joined.groupBy("has_alzheimers").avg("Cost")
summary = summary.withColumnRenamed("avg(Cost)","average_cost")

summary.display()

# COMMAND ----------

summary.write.mode("overwrite").saveAsTable("summary_table")

# COMMAND ----------

