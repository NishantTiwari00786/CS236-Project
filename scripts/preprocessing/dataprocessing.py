# 1.3 part of project phase 1. 





# cleanliness required in hotel-booking dataset: 

# 1. removing 405 null values we found in the country column
# https://stackoverflow.com/questions/44163153/how-to-drop-rows-with-nulls-in-one-column-pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data-processing").getOrCreate() # generating spark session

from pyspark.sql.functions import col, when 



df_customer = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)





df_hotel_cleaned = df_hotel.withColumn("country", when(col("country").isNull(), "Unknown").otherwise(col("country")))   


# removing the duplicate row that we found in the hotel-booking dataset: 

df_hotel_cleaned = df_hotel_cleaned.dropDuplicates()

# handing outliers 

df_hotel_cleaned = df_hotel_cleaned.filter((col("avg_price_per_room")<= 1000) & (col("stays_in_week_nights")<= 40))



# displaying the amount of duplicates removed

before_count = df_hotel.count()
after_count = df_hotel_cleaned.count()
print(f"Number of rows before cleaning: {before_count}")
print(f"Number of rows after cleaning: {after_count}")
print(f"Number of rows removed: {before_count - after_count}")

# saving the cleaned hotel dataset to a new csv files 

df_hotel_cleaned.write.csv("data/clean/hotel-booking-cleaned.csv", header = True, mode = "overwrite")

df_customer.write.csv("data/clean/customer-reservations-cleaned.csv", header = True, mode = "overwrite")



