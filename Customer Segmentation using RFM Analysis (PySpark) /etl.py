2️⃣ Data Conversion: Excel ➡️ CSV

# etl.py — Load, Clean, and Save Data

import pandas as pd

# Read the raw retail data from an Excel file (source format)
df_excel=pd.read_excel("online_retail_II.xlsx")

# Convert the Excel sheet to CSV so Spark can easily consume it
df_excel.to_csv("retail_project_data.csv", index=False)




3️⃣ Load Data in Spark

# Load CSV data into Spark DataFrame
df=spark.read.csv("retail_project_data.csv",header=True)

# Show first 5 rows and infer schema
df.show(5)
df.printSchema()





Check the initial schema - note most columns are loaded as strings, which needs correction.

4️⃣ Redefine Schema for Proper Data Types

# The default schema treats many fields as strings: we need to specify actual data types

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Country", StringType(), True)
])

df=spark.read.csv("retail_project_data.csv",header=True,schema=schema)      
df.show(5)
df.printSchema()




5️⃣ Data Overview and Null Check

# Summarize dataset: count, mean, stddev, min, max for each numeric column
df.describe().collect()

# Check number of nulls in each column
from pyspark.sql.functions import col, sum as _sum
df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()





6️⃣ Data Cleaning


# Remove problematic rows according to business rules:
# 1. Remove transactions with missing Customer ID.
# 2. Remove cancellation invoices (Invoice starting with 'C').
# 3. Remove rows with negative/zero Quantity or Price.

df=df.filter(col('Customer ID').isNotNull())
df=df.filter(~col('Invoice').startswith('C'))          # Tilde (~): NOT operator in PySpark
df=df.filter(df.Quantity>0).filter(df.Price>0) 

7️⃣ Feature Engineering: Revenue


# Aggregate: total revenue per Country and InvoiceDate

df.groupBy('Country','InvoiceDate').sum('Revenue').withColumnRenamed('sum_Rev','total_Rev').show() 






Revenue by Year, Month, and Country:

from pyspark.sql.functions import to_date, month, year

# Extract year and month for time-based analysis
df=df.withColumn('InvoiceDate',to_date(col('InvoiceDate'), 'yyyy-MM-dd'))
df=df.withColumn('month',month(col('InvoiceDate')))
df=df.withColumn('year',year(col('InvoiceDate')))


# Aggregate revenue over year, month, and country
df.groupBy('year','month','Country').sum('Revenue').withColumnRenamed('sum_Rev','total_Rev').orderBy('year','month','Country').show()








9️⃣ Save Cleaned Results for Downstream Use

# Save cleaned DataFrame and aggregated DataFrames as Parquet for efficient storage/reloading
df.write.mode('overwrite').parquet('cleaned_DataFrame')

agg_df_RbyCC=df.groupBy('Country','InvoiceDate').sum('Revenue').withColumnRenamed('sum_Rev','total_Rev')

agg_df_RbyMC=df.groupBy('year','month','Country').sum('Revenue').withColumnRenamed('sum_Rev','total_Rev').orderBy('year','month','Country')

agg_df_RbyCC.write.mode('overwrite').parquet('RbyCC')
agg_df_RbyMC.write.mode('overwrite').parquet('RbyMC')






Reload parquet with: spark.read.parquet("filename")

🔟 Top-N Customers by Revenue

# Find top 10 customers by total revenue

df_cleaned=spark.read.parquet('cleaned_DataFrame')
top_N_cust=df_cleaned.groupBy('Customer ID').sum('Revenue').orderBy(col('sum(Revenue)').desc()).limit(10).show() 


