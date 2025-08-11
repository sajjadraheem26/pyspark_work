import os

# Set PySpark environment variables to use Anaconda Python (customize these paths as needed)

os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/bin/python'


from pyspark.sql import SparkSession

# Start a new Spark session for this retail project
spark=SparkSession.builder.appName('Retail_Project1').getOrCreate()
