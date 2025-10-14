# python file specifcally for exploratorion and analysis of datasets we are working with 
# https://medium.com/dataseries/an-eda-checklist-800beeaee555 : used this link as benchmark for EDA process 



from pyspark.sql import SparkSession #importing spark session 

spark = SparkSession.builder.appName("LoadingDataSet").getOrCreate() # generating spark session



# Loading both the datasets

df_customer = spark.read.csv("/Users/nishanttiwari/Desktop/CS236-Project/data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)



# firstly doing the row counts: 


print("Customer rows:", df_customer.count())
print("Hotel rows:", df_hotel.count())


# checking for distinct values in each column we think is important: 

# customer_reservations dataset distinct values: 

distinct_bookingID = df_customer.select("Booking_ID").distinct().count()

print("Distinct Booking_ID: ", distinct_bookingID) # the result is 36275

distinct_bookingStatus = df_customer.select("Booking_Status").distinct().count()
print("Distinct Booking_Status: ", distinct_bookingStatus) 

distinct_marketSegment = df_customer.select("market_segment_type").distinct().count()
print("Distinct Market_Segment: ", distinct_marketSegment)


distinct_arrivalMonth = df_customer.select("arrival_month").distinct().count()
print("Distinct Arrival Month: ", distinct_arrivalMonth)

distinct_arrivalyear = df_customer.select("arrival_year").distinct().count()
print("Distinct Arrival Year: ", distinct_arrivalyear)


# hotel-booking dataset distinct values: 

distinct_Hoteltype = df_hotel.select("hotel").distinct().count()
print("Distinct Hotel Type: ", distinct_Hoteltype)

distinct_arrivalYEARHOTEL = df_hotel.select("arrival_year").distinct().count()
print("Distinct Arrival Year Hotel: ", distinct_arrivalYEARHOTEL)

distinct_arrivalMONTHHOTEL = df_hotel.select("arrival_month").distinct().count()
print("Distinct Arrival Month Hotel: ", distinct_arrivalMONTHHOTEL)

distinct_marketSegmentHOTEL = df_hotel.select("market_segment_type").distinct().count()
print("Distinct Market Segment Hotel: ", distinct_marketSegmentHOTEL)

distinct_country = df_hotel.select("country").distinct().count()
print("Distinct Country: ", distinct_country)


distinct_hotelRows = df_hotel.distinct().count()
totalRows = df_hotel.count()
print("Distinct Hotel Rows: ", distinct_hotelRows, "out of ", totalRows) 


# checking for null values in customer-reservations dataset: 

# https://stackoverflow.com/questions/37262762/filter-pyspark-dataframe-column-with-none-value: used this for reference 
# https://sparkbyexamples.com/pyspark/pyspark-filter-rows-with-null-values/: used this for reference

from pyspark.sql.functions import col, sum 

counter_null_customer = df_customer.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_customer.columns])
counter_null_customer.show()

# checking for null values in hotel-booking dataset:

counter_null_hotel = df_hotel.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_hotel.columns])
counter_null_hotel.show()



# analyzing distribution of numerical columns in the datasets: 
# https://www.projectpro.io/recipes/explain-kurtosis-min-max-and-mean-aggregate-functions-pyspark-databricks#:~:text=The%20PySpark%20min%20and%20max,RDD%20(Resilient%20Distributed%20Dataset).&text=The%20PySpark%20mean%20function%20calculates%20the%20average%20value%20of%20a%20given%20dataset.: used this for reference

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aggregate.html

from pyspark.sql.functions import mean, max, stddev, min 

# customer-reservations dataset: 

df_customer.select(
    mean("lead_time").alias("mean_lead_time"),
    max("lead_time").alias("max_lead_time"),
    min("lead_time").alias("min_lead_time"),
    stddev("lead_time").alias("stddev_lead_time")
).show()

df_customer.select(   
    mean("stays_in_week_nights").alias("mean_stays_in_week_nights"),
    max("stays_in_week_nights").alias("max_stays_in_week_nights"),
    min("stays_in_week_nights").alias("min_stays_in_week_nights"),
    stddev("stays_in_week_nights").alias("stddev_stays_in_week_nights")
).show()

df_customer.select(   
    mean("stays_in_weekend_nights").alias("mean_stays_in_weekend_nights"),
    max("stays_in_weekend_nights").alias("max_stays_in_weekend_nights"),
    min("stays_in_weekend_nights").alias("min_stays_in_weekend_nights"),
    stddev("stays_in_weekend_nights").alias("stddev_stays_in_weekend_nights")
).show()

df_customer.select(   
    mean("avg_price_per_room").alias("mean_avg_price_per_room"),
    max("avg_price_per_room").alias("max_avg_price_per_room"),
    min("avg_price_per_room").alias("min_avg_price_per_room"),
    stddev("avg_price_per_room").alias("stddev_avg_price_per_room")
).show()


# hotel -booking dataset:

df_hotel.select(
    mean("lead_time").alias("mean_lead_time"),
    max("lead_time").alias("max_lead_time"),
    min("lead_time").alias("min_lead_time"),
    stddev("lead_time").alias("stddev_lead_time")
).show()
    
df_hotel.select(  
    mean("stays_in_week_nights").alias("mean_stays_in_week_nights"),
    max("stays_in_week_nights").alias("max_stays_in_week_nights"),
    min("stays_in_week_nights").alias("min_stays_in_week_nights"),
    stddev("stays_in_week_nights").alias("stddev_stays_in_week_nights")
).show()
  
df_hotel.select(  
    mean("stays_in_weekend_nights").alias("mean_stays_in_weekend_nights"),
    max("stays_in_weekend_nights").alias("max_stays_in_weekend_nights"),
    min("stays_in_weekend_nights").alias("min_stays_in_weekend_nights"),
    stddev("stays_in_weekend_nights").alias("stddev_stays_in_weekend_nights")
).show()

df_hotel.select( 
    mean("avg_price_per_room").alias("mean_avg_price_per_room"),
    max("avg_price_per_room").alias("max_avg_price_per_room"),
    min("avg_price_per_room").alias("min_avg_price_per_room"),
    stddev("avg_price_per_room").alias("stddev_avg_price_per_room")
).show()



# check for the duplicate row that we found the hotel-booking dataset:

# used this for reference: https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/

from pyspark.sql.functions import count 
df_hotel.groupBy(df_hotel.columns).agg(count("*").alias("count")).filter(col("count") > 1).show()

# just a check for duplicate row in customer dataset: 
#from pyspark.sql.functions import count 
#df_customer.groupBy(df_customer.columns).agg(count("*").alias("count")).filter(col("count") > 1).show()
# 


# Distinction between market_segment_type in customer_reservation and hotel booking dataset: 

df_customer.groupBy("market_segment_type").count().show()

df_hotel.groupBy("market_segment_type").count().show()




# Numerical number of booking that we cancelled/ not cancelled: 

df_customer.groupBy("Booking_Status").count().show()



