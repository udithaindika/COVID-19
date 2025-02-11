# Databricks notebook source
import pandas as pd
import os
import csv

csv_folder_path = "./csse_covid_19_data/csse_covid_19_daily_reports"
abs_csv_folder_path = os.path.abspath(csv_folder_path);
print("Absolute Path : ", abs_csv_folder_path);

csv_files = [os.path.join(abs_csv_folder_path, f) for f in os.listdir(abs_csv_folder_path) if f.endswith(".csv")]
print(f"Number of files to be processed :{len(csv_files)}")

def proceed():
    confirmation = input("⚠️ This action is irreversible. Type 'YES' to proceed: ")
    if confirmation.upper() != "YES":
        raise Exception("🚫 Execution aborted!")


# COMMAND ----------

import os
import concurrent.futures

def read_first_line(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return (file_path, file.readline().strip())  # Read first line and remove newline
    except Exception as e:
        return (file_path, f"Error: {e}")
    
first_lines = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    results = executor.map(read_first_line, csv_files)
    first_lines = list(results)

# Keep a mapping of header name to list of files that have the same header, this is to because then we can load all the files using the spark, rather than loading then one by one. which takes a long time. 
# First time I tried loading the files one by one around 1143, It took more than 1 hour, but still didnt complete the jobs. ie. first read file, load it to a df, and then append it a root df. and once all files are loaded, save that to a table. 
header_to_file = {}

for (file_path, first_line) in first_lines:
    if first_line not in header_to_file:
        header_to_file[first_line] = [];
    header_to_file[first_line].append(f"file:{file_path}");



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS covid_stat;
# MAGIC USE  covid_stat;
# MAGIC -- ALTER TABLE daily_reports RENAME TO daily_reports_old;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import input_file_name, monotonically_increasing_id, split, element_at, concat_ws, to_timestamp , col , when

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
    DoubleType,
    LongType,
)
import os

proceed();


# CSV files structure, and whats in side of them, The issue here is the column names and the column index are no consistent. so there could be an issue if load all the files as it is. So we keep a name mapping of different column names and put that in to a schema.

# Following are the column headings found in the CSV files.
# FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incident_Rate,Case_Fatality_Ratio
# FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key
## All the unique columes names in all files
#{'Admin2', 'Country/Region', 'Province/State', 'Last Update', 'Deaths', 'Longitude', 'Long_', 'Lat', 'Combined_Key', 'Case_Fatality_Ratio', 'FIPS', 'Confirmed', 'Incidence_Rate', 'Province_State', 'Active', 'Latitude', 'Incident_Rate', 'Recovered', 'Last_Update', 'Country_Region', 'Case-Fatality_Ratio'}


# FIPS: US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.
# Admin2: County name. US only.
# Province_State: Province, state or dependency name.
# Country_Region: Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.
# Last Update: MM/DD/YYYY HH:mm:ss (24 hour format, in UTC).
# Lat and Long_: Dot locations on the dashboard. All points (except for Australia) shown on the map are based on geographic centroids, and are not representative of a specific address, building or any location at a spatial scale finer than a province/state. Australian dots are located at the centroid of the largest city in each state.
# Confirmed: Counts include confirmed and probable (where reported).
# Deaths: Counts include confirmed and probable (where reported).
# Recovered: Recovered cases are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from COVID Tracking Project. We stopped to maintain the recovered cases (see Issue #3464 and Issue #4465).
# Active: Active cases = total cases - total recovered - total deaths. This value is for reference only after we stopped to report the recovered cases (see Issue #4465)
# Incident_Rate: Incidence Rate = cases per 100,000 persons.
# Case_Fatality_Ratio (%): Case-Fatality Ratio (%) = Number recorded deaths / Number cases.
# All cases, deaths, and recoveries reported are based on the date of initial report. Exceptions to this are noted in the "Data Modification" and "Retrospective reporting of (probable) cases and deaths" subsections below.


schema = StructType(
    [
        StructField("FIPS", StringType(), True),
        StructField("AdminFlag", StringType(), True),
        StructField("StateOrProvince", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("LastUpdated", TimestampType(), True),
        StructField("Lat", DecimalType(9, 6), True),
        StructField("Long_", DecimalType(9, 6), True),
        StructField("Confirmed", LongType(), True),
        StructField("Deaths", LongType(), True),
        StructField("Recovered", LongType(), True),
        StructField("Active", LongType(), True),
        StructField("IncidentRate", DoubleType(), True),
        StructField("CaseFatalityRatio", DoubleType(), True),
        StructField("Source", StringType(), True),
    ]
)

column_mapping = {
    "FIPS": "FIPS",
    "Admin2": "AdminFlag",
    "Province_State": "StateOrProvince",
    "Province/State": "StateOrProvince",
    "Country_Region": "Country",
    "Country/Region": "Country",
    "Last_Update": "LastUpdated",
    "Last Update": "LastUpdated",
    "Lat": "Lat",
    "Latitude" : "Lat",
    "Long_": "Long_",
    "Longitude" : "Long_",
    "Confirmed": "Confirmed",
    "Deaths": "Deaths",
    "Recovered": "Recovered",
    "Active": "Active",
    "Incident_Rate": "IncidentRate",
    "Incidence_Rate": "IncidentRate",
    "Case_Fatality_Ratio": "CaseFatalityRatio",
    "Case-Fatality_Ratio": "CaseFatalityRatio",
    "Source": "Source",
}


# Set preferred Parquet file size (~256MB)
spark.conf.set("parquet.block.size", 256 * 1024 * 1024)
# Ensure partitions generate reasonable file sizes
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")

root_df = spark.createDataFrame([], schema)

for key in header_to_file:
    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("pathGlobFilter", "*.csv")\
        .load(header_to_file[key])\
        .withColumn("Source", concat_ws (":",input_file_name(),monotonically_increasing_id()))\
        .drop("Combined_Key")

    new_columns = [column_mapping.get(col, col) for col in df.columns]
    df = df.toDF(*new_columns)



    columns_to_cast = {
        "LastUpdated": ("yyyy-MM-dd HH:mm:ss", to_timestamp),
        "Lat": (DecimalType(9, 6), col),
        "Long_": (DecimalType(9, 6), col),
        "Confirmed": (LongType(), col),
        "Deaths": (LongType(), col),
        "Recovered": (LongType(), col),
        "Active": (LongType(), col),
        "IncidentRate": (DoubleType(), col),
        "CaseFatalityRatio": (DoubleType(), col),
    }

    # df_fixed = df.withColumn("LastUpdated", when((col("LastUpdated").isNotNull()) & (col("LastUpdated") != ""), to_timestamp(col("LastUpdated"), "yyyy-MM-dd HH:mm:ss")).otherwise(col("LastUpdated")))\
    #             .withColumn("Lat", when(col("Lat").isNotNull(), col("Lat").cast(DecimalType(9, 6))).otherwise(col("Lat")))\
    #             .withColumn("Long_", when(col("Long_").isNotNull(), col("Long_").cast(DecimalType(9, 6))).otherwise(col("Long_")))\
    #             .withColumn("Confirmed", when(col("Confirmed").isNotNull(), col("Confirmed").cast(LongType())).otherwise(col("Confirmed")))\
    #             .withColumn("Deaths", when(col("Deaths").isNotNull(), col("Deaths").cast(LongType())).otherwise(col("Deaths")))\
    #             .withColumn("Recovered", when(col("Recovered").isNotNull(), col("Recovered").cast(LongType())).otherwise(col("Recovered")))\
    #             .withColumn("Active", when(col("Active").isNotNull(), col("Active").cast(LongType())).otherwise(col("Active")))\
    #             .withColumn("IncidentRate", when(col("IncidentRate").isNotNull(), col("IncidentRate").cast(DoubleType())).otherwise(col("IncidentRate")))\
    #             .withColumn("CaseFatalityRatio", when(col("CaseFatalityRatio").isNotNull(), col("CaseFatalityRatio").cast(DoubleType())).otherwise(col("CaseFatalityRatio")))

    for column, (cast_type, cast_function) in columns_to_cast.items():
            if column in df.columns:
                if cast_function == to_timestamp:
                    df = df.withColumn(column, when(col(column).isNotNull(), to_timestamp(col(column), cast_type)).otherwise(col(column)))
                else:
                    df = df.withColumn(column, when(col(column).isNotNull(), col(column).cast(cast_type)).otherwise(col(column)))

    # df_fixed = df.withColumn("LastUpdated", to_timestamp(col("LastUpdated"), "yyyy-MM-dd HH:mm:ss"))\
    #                 .withColumn("Lat", col("Lat").cast(DecimalType(9, 6)))\
    #                 .withColumn("Long_", col("Long_").cast(DecimalType(9, 6)))\
    #                 .withColumn("Confirmed", col("Confirmed").cast(LongType()))\
    #                 .withColumn("Deaths", col("Deaths").cast(LongType()))\
    #                 .withColumn("Recovered", col("Recovered").cast(LongType()))\
    #                 .withColumn("Active", col("Active").cast(LongType()))\
    #                 .withColumn("IncidentRate", col("IncidentRate").cast(DoubleType()))\
    #                 .withColumn("CaseFatalityRatio", col("CaseFatalityRatio").cast(DoubleType()))

    df_correct_schema = spark.createDataFrame(df.rdd, schema)
    root_df = root_df.unionByName(df_correct_schema, allowMissingColumns=True)

# # Convert to Delta for better performance
root_df.write.format("delta")\
      .mode("overwrite")\
      .option("overwriteSchema", "true")\
      .partitionBy("Country")\
      .saveAsTable("daily_reports")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE EXTENDED daily_reports
# MAGIC -- DESCRIBE DETAIL daily_reports
# MAGIC -- DESCRIBE DETAIL daily_reports
# MAGIC SHOW TABLES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT COUNT(*) from daily_reports
# MAGIC -- SELECT * FROM daily_reports limit 100
# MAGIC -- SELECT * FROM daily_reports where Country = 'US'
# MAGIC --ALTER TABLE daily_reports ALTER COLUMN Country SET NOT NULL;
# MAGIC -- DESCRIBE DETAIL daily_reports;
# MAGIC DESCRIBE daily_reports
# MAGIC --SHOW TBLPROPERTIES daily_reports;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
