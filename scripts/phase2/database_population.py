from pyspark.sql import SparkSession 



# initializing spark session with JDBC driver 

spark = SparkSession.builder \
    .appName("database-population") \
    .config("spark.jars", "/Users/nishanttiwari/Desktop/postgresql-42.7.8.jar")\
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


# writing to postgres and overwriting if the table already exists

customer_df.write.jdbc(jdbc_url, "customer_reservations", mode="overwrite", properties=connection_properties)
hotel_df.write.jdbc(jdbc_url, "hotel_bookings", mode="overwrite", properties=connection_properties)
unified_df.write.jdbc(jdbc_url, "unified_bookings", mode="overwrite", properties=connection_properties)


# verifying the data has been written correctly by reading back from the database

print("Data successfully written to PostgreSQL database.")

print({"customer_reservations": customer_df.count(), "hotel_bookings": hotel_df.count(), "unified_bookings": unified_df.count()})

spark.stop()







# https://www.geeksforgeeks.org/python/pyspark-apply-custom-schema-to-a-dataframe/ used this for reference on applying schema