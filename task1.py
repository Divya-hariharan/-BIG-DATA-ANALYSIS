# TASK - 1: BIG DATA ANALYSIS USING PYSPARK

# Step 1: Install PySpark
!pip install pyspark

# Step 2: Upload CSV file
from google.colab import files
uploaded = files.upload()

# Step 3: Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

# Step 4: Create Spark Session
spark = SparkSession.builder.appName("HealthDataAnalysis").getOrCreate()

# Step 5: Load the Dataset
df = spark.read.csv("healthcare_large.csv", header=True, inferSchema=True)

# Step 6: Display initial data
df.show(15)
df.printSchema()

# Step 7: Drop null values
df_clean = df.dropna()
df_clean.show()

# Step 8: Count Patients by Disease
print("Count of Patients by Disease:")
df_clean.groupBy("Disease").count().orderBy("count", ascending=False).show()

# Step 9: Average Age per Disease
print(" Average Age per Disease:")
df_clean.groupBy("Disease") \
    .agg(round(avg("Age"), 2).alias("Average_Age")) \
    .orderBy("Average_Age", ascending=False) \
    .show()

# Step 10: Average Cholesterol by Diabetes Status
print(" Average Cholesterol for Diabetic vs Non-Diabetic:")
df_clean.groupBy("Diabetes") \
    .agg(round(avg("Cholesterol"), 2).alias("Avg_Cholesterol")) \
    .show()

# Step 11: Count of Patients Over Age 60
over_60_count = df_clean.filter(df_clean["Age"] > 60).count()
print(f"Number of patients over age 60: {over_60_count}")
