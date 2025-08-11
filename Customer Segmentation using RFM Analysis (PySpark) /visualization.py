1️⃣4️⃣ RFM Segment Distribution Visualization

RFM_segmented.groupBy("Segment").count().orderBy(col("count").desc()).show()




import matplotlib.pyplot as plt


# Convert Spark DataFrame to Pandas for plotting
segment_counts = RFM_segmented.groupBy("Segment").count().toPandas()
# Sort by count descending
segment_counts = segment_counts.sort_values(by="count", ascending=False)


plt.figure(figsize=(10,6))
plt.bar(segment_counts["Segment"], segment_counts["count"], color="skyblue")
plt.xticks(rotation=45)
plt.xlabel("Customer Segment")
plt.ylabel("Number of Customers")
plt.title("RFM Customer Segmentation Distribution")
plt.tight_layout()
plt.show()
