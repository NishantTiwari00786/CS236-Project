from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# setup paths to find project files automatically
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../"))
load_dotenv(os.path.join(project_root, ".env"))

# initializing spark session with JDBC driver
jar_path = os.path.join(project_root, "JARS", "postgresql-42.7.8.jar")

spark = SparkSession.builder \
    .appName("database-population") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# postgreSQL connection information
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASS")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

connection_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# loading the datasets from CSV Files
data_dir = os.path.join(project_root, "data")

customer_df = spark.read.csv(os.path.join(data_dir, "clean/customer-reservations-cleaned.csv"), header=True, inferSchema=True)
hotel_df = spark.read.csv(os.path.join(data_dir, "clean/hotel-booking-cleaned.csv"), header=True, inferSchema=True)
unified_df = spark.read.csv(os.path.join(data_dir, "unified/unified-dataset_fix.csv"), header=True, inferSchema=True)

# writing to postgres and overwriting if the table already exists
customer_df.write.jdbc(jdbc_url, "customer_reservations", mode="overwrite", properties=connection_properties)
hotel_df.write.jdbc(jdbc_url, "hotel_bookings", mode="overwrite", properties=connection_properties)
unified_df.write.jdbc(jdbc_url, "unified_bookings", mode="overwrite", properties=connection_properties)

# verifying the data 
print("Data successfully written to PostgreSQL database.")
print({"customer_reservations": customer_df.count(), "hotel_bookings": hotel_df.count(), "unified_bookings": unified_df.count()})

spark.stop()
# https://www.geeksforgeeks.org/python/pyspark-apply-custom-schema-to-a-dataframe/ used this for reference on applying schema