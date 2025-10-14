from pyspark.sql import SparkSession

# Create a Spark session with logging config
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:logs/log4j.properties") \
    .getOrCreate()

#check: print Spark version
print("Spark version:", spark.version)

# Stop the session
spark.stop()