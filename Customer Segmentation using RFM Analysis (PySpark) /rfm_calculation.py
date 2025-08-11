# rfm_calculation.py — RFM Metrics & Scores

1️⃣1️⃣ RFM Analysis: Recency, Frequency, Monetary

What is RFM?

Recency: How recently did the customer buy? (days since last purchase)

Frequency: How often do they buy? (number of purchases)

Monetary: How much do they spend? (total revenue)






# RFM: Key metrics for customer segmentation

from pyspark.sql.functions import max, current_date, datediff,lit,countDistinct

# Find the most recent InvoiceDate in the dataset to define "today"
max_date = df_cleaned.select(max("InvoiceDate")).collect()[0][0]

# Recency: Days since last purchase for each customer
recency_df = df_cleaned.groupBy("Customer ID").agg(datediff(lit(max_date), max("InvoiceDate")).alias("Recency")).orderBy(col('Recency').desc())

# Frequency: Count of unique invoices per customer
frequency_df = df_cleaned.groupBy("Customer ID").agg(countDistinct("Invoice").alias("Frequency")).orderBy(col('Frequency'))

# Monetary: Total revenue per customer
monetary_df=df_cleaned.groupBy('Customer ID').sum('Revenue').withColumnRenamed('sum(Revenue)','Monetary').orderBy(col('Monetary').desc())




Combine into RFM DataFrame:

# Join R, F, M metrics into a single DataFrame

RFM_df=recency_df.join(frequency_df, 'Customer ID').join(monetary_df,'Customer ID')
RFM_df.show(5)




1️⃣2️⃣ Assign RFM Scores

from pyspark.sql.functions import ntile,concat_ws
from pyspark.sql.window import Window


# Score: assign 1-5 bucket to each metric (1: best, 5: worst for recency; 5: best for frequency/monetary)


recency_score=RFM_df.withColumn('recency_score', ntile(5).over(Window.orderBy(col("Recency").asc())))
frequency_score=RFM_df.withColumn('frequency_score', ntile(5).over(Window.orderBy(col("Frequency").desc())))
monetary_score=RFM_df.withColumn('monetary_score', ntile(5).over(Window.orderBy(col("Monetary").desc())))

# Combine all scores for each customer
RFM_score=recency_score.join(frequency_score, on='Customer ID').join(monetary_score, on='Customer ID')


# Concatenated and total RFM score
RFM_score_combibed=RFM_score.withColumn('RFM_score_combined',(concat_ws("",col('recency_score'),col('frequency_score'),col('monetary_score'))))
RFM_score_total=RFM_score.withColumn('RFM_score_total',col('recency_score')+col('frequency_score')+col('monetary_score'))
