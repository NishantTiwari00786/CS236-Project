# file for merging the cleaned datasets into one single dataset that is placed under data/unified 


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, trim
spark = SparkSession.builder.appName("merge-dataset").getOrCreate() # generating spark session


# using spark to read through the cleaned datasets


df_customer_cleaned = spark.read.csv("data/clean/customer-reservations-cleaned.csv", header = True, inferSchema = True)
df_hotel_cleaned = spark.read.csv("data/clean/hotel-booking-cleaned.csv", header = True, inferSchema = True)


# double checking the columns on both datasets 

df_customer_cleaned.printSchema()
df_hotel_cleaned.printSchema()



# The result is as follows. It shows that Booking_ID is unique to the customer dataset, while in hotel dataset,  hotel: string, arrival_date_week_number, country, and email are unique. 
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html : used this for reference
# https://www.geeksforgeeks.org/python/pyspark-merge-two-dataframes-with-different-columns-or-schema/ - used this for refeerence
# https://stackoverflow.com/questions/51439843/unknown-vs-any

# getting the result of unique columns in both datasets
cols_customer = set(df_customer_cleaned.columns)
cols_hotel = set(df_hotel_cleaned.columns)


# addying missing colums to both datasets (that are missing in each dataset) 
missing_col_in_customer = cols_hotel - cols_customer
missing_col_in_hotel = cols_customer - cols_hotel


#setting the missing colums to unkown values 

for col in missing_col_in_customer:
    df_customer_cleaned = df_customer_cleaned.withColumn(col, lit("Unknown"))
for col in missing_col_in_hotel:
    df_hotel_cleaned = df_hotel_cleaned.withColumn(col, lit("Unknown"))


# lowercasing the letters of columns name in both datasets to ensure consistency 
from pyspark.sql.functions import col,  lower

df_customer_cleaned = df_customer_cleaned.toDF(*[c.lower() for c in df_customer_cleaned.columns])
df_hotel_cleaned = df_hotel_cleaned.toDF(*[c.lower() for c in df_hotel_cleaned.columns])

# lowercasing all the string columns 

string_columns_customer = ["booking_status", "market_segment_type"]
for c in string_columns_customer:
    if c in df_customer_cleaned.columns:
        df_customer_cleaned = df_customer_cleaned.withColumn(c, lower(col(c)))
        
string_columns_hotel = ["hotel", "country", "market_segment_type", "booking_status", "email"] 
for c in string_columns_hotel:
    if c in df_hotel_cleaned.columns:
        df_hotel_cleaned = df_hotel_cleaned.withColumn(c, lower(col(c)))

# we also need to ensure that both datasets in their indivdiaul column have the same data type.    



df_customer_cleaned = df_customer_cleaned.withColumn("avg_price_per_room", col("avg_price_per_room").cast("double"))
df_hotel_cleaned = df_hotel_cleaned.withColumn("avg_price_per_room", col("avg_price_per_room").cast("double"))

#cancelled/not cancelled alignment
# in customer dataet, we are using, cancelled and not cancelled but on hotel dataset, we are using 1 and 0. We need to align them.
from pyspark.sql.functions import when

df_customer_cleaned = df_customer_cleaned.withColumn("booking_status", when(trim(lower(col("booking_status"))).contains("cancel"), "cancelled").otherwise("not cancelled"))

df_hotel_cleaned = df_hotel_cleaned.withColumn("booking_status", when(col("booking_status") == 1, "cancelled").when(col("booking_status") == 0, "not cancelled").otherwise("not cancelled"))

# month alignment
# in customer dataset, we are using full month names, but in hotel dataset, we are using month numbers. We need to align them.
from pyspark.sql.functions import when, col 

month_mapping = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12
}

for month_name, month_num in month_mapping.items():
    df_customer_cleaned = df_customer_cleaned.withColumn(
        "arrival_month",
        when(col("arrival_month") == month_name, month_num).otherwise(col("arrival_month"))
    )

# cast to int on both datasets
df_customer_cleaned = df_customer_cleaned.withColumn("arrival_month", col("arrival_month").cast("int"))
df_hotel_cleaned = df_hotel_cleaned.withColumn("arrival_month", col("arrival_month").cast("int"))

# adding a check for hotel dataset as well to make sure that it also in int. 
df_hotel_cleaned = df_hotel_cleaned.withColumn(
    "arrival_month", col("arrival_month").cast("int")
)



# reordeing them to prevent mismatch 
# https://stackoverflow.com/questions/62490026/how-to-merge-dataframes-keeping-order-in-spark-or-python used this for reference
final_columns = sorted(df_customer_cleaned.columns)  # soting them alphabetically 
df_customer_cleaned = df_customer_cleaned.select(final_columns)
df_hotel_cleaned = df_hotel_cleaned.select(final_columns)




# merging both datasets
df_unified = df_customer_cleaned.unionByName(df_hotel_cleaned)
df_unified.show()
df_unified.printSchema()


# saving it to the data/unified 

df_unified.write.csv("data/unified/unified-dataset.csv", header = True, mode = "overwrite")


