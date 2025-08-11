1️⃣3️⃣ Segment Customers

#segement analysis::
from pyspark.sql.functions import when

# Assign each customer to a segment based on their total RFM score
RFM_segmented = RFM_score_total.withColumn(
    "Segment",
    when(col("RFM_score_total") >= 13, "Champions") \
    .when((col("RFM_score_total") >= 10) & (col("RFM_score_total") <= 12), "Loyal Customers") \
    .when((col("RFM_score_total") >= 7) & (col("RFM_score_total") <= 9), "Potential Loyalist") \
    .when((col("RFM_score_total") >= 4) & (col("RFM_score_total") <= 6), "Needs Attention") \
    .otherwise("At Risk")
)
RFM_segmented.show(20)

