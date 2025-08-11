Customer Segmentation using RFM Analysis (PySpark)
📌 Project Overview
This project performs RFM (Recency, Frequency, Monetary) analysis on transactional sales data to segment customers into different categories.
The analysis is implemented using PySpark for distributed data processing and includes customer segmentation visualization.

📊 What is RFM Analysis?
Recency (R) → How recently a customer made a purchase.

Frequency (F) → How often a customer makes purchases.

Monetary (M) → How much money a customer spends.

RFM analysis helps identify different customer types such as loyal customers, champions, at-risk customers, and more, enabling targeted marketing strategies.

📂 Project Structure
bash
Copy
Edit
├── proj.ipynb           # Jupyter Notebook with PySpark implementation
├── data/                # (Optional) Folder for dataset
├── README.md            # Project documentation
├── requirements.txt     # Python dependencies
⚙️ Technologies Used
PySpark → Data processing & RFM computation

Pandas → Small-scale data handling

Matplotlib → Data visualization

Jupyter Notebook → Interactive coding & documentation

🚀 Steps in the Project
Data Cleaning

Removed null values and invalid records.

Generated Revenue column (Quantity * UnitPrice).

RFM Metric Calculation

Calculated Recency, Frequency, and Monetary for each customer.

Used Window functions for percentile scoring.

Scoring & Segmentation

Assigned R, F, and M scores (1–5 scale).

Combined into total and concatenated RFM score.

Customer Segmentation

Created segments based on RFM patterns.

Example: 555 → Champions, 155 → At Risk.

Visualization

Bar plot showing number of customers per segment.

📈 Example Output
Segment Distribution

Segment	Count
Loyal Customers	1118
Potential Loyalist	1058
Champions	914
Needs Attention	881
At Risk	341

💡 How to Run
bash
Copy
Edit
# Install dependencies
pip install -r requirements.txt

# Start Jupyter Notebook
jupyter notebook proj.ipynb
📜 License
This project is open-source and available under the MIT License.

📌 Conclusion
Through RFM analysis, we identified valuable customer segments such as Champions and Loyal Customers who are key to sustained revenue, as well as At Risk customers who need targeted retention efforts.
By visualizing these segments, the business can prioritize marketing campaigns, improve customer retention, and maximize lifetime value.

Future enhancements:

Automate the segmentation with scheduled PySpark jobs.

Add advanced visual dashboards (e.g., Plotly, Tableau).

Integrate with CRM tools for targeted marketing.

