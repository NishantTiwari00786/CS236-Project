from pyspark.sql import SparkSession 



# initializing spark session with JDBC driver 

spark = SparkSession.builder \
    .appName("database-population") \
    .config("spark.jars", "path/to/postgresql-42.7.3")\
    .getOrCreate()
    
# postgreSQL connection information 

jdbc_url = "jdbc:postgresql://localhost:5432/bookings"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# loading the datasets from CSV Files 

customer_df = spark.read.csv("data/clean/customer-reservations-cleaned.csv", header=True, inferSchema=True )

hotel_df = spark.read.csv("data/clean/hotel-booking-cleaned.csv", header=True, inferSchema=True )

unified_df = spark.read.csv("data/unified/unified-dataset_fix.csv", header=True, inferSchema=True)



