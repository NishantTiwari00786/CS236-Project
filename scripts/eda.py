# python file specifcally for exploratorion and analysis of datasets we are working with 
# https://medium.com/dataseries/an-eda-checklist-800beeaee555 : used this link as benchmark for EDA process 



from pyspark.sql import SparkSession #importing spark session 

spark = SparkSession.builder.appName("LoadingDataSet").getOrCreate() # generating spark session



# Loading both the datasets

df_customer = spark.read.csv("../data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("../data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)



# # firstly doing the row counts: 


# print("Customer rows:", df_customer.count())
# print("Hotel rows:", df_hotel.count())


# # checking for distinct values in each column we think is important: 

# # customer_reservations dataset distinct values: 

# distinct_bookingID = df_customer.select("Booking_ID").distinct().count()

# print("Distinct Booking_ID: ", distinct_bookingID) # the result is 36275

# distinct_bookingStatus = df_customer.select("Booking_Status").distinct().count()
# print("Distinct Booking_Status: ", distinct_bookingStatus) 

# distinct_marketSegment = df_customer.select("market_segment_type").distinct().count()
# print("Distinct Market_Segment: ", distinct_marketSegment)


# distinct_arrivalMonth = df_customer.select("arrival_month").distinct().count()
# print("Distinct Arrival Month: ", distinct_arrivalMonth)

# distinct_arrivalyear = df_customer.select("arrival_year").distinct().count()
# print("Distinct Arrival Year: ", distinct_arrivalyear)


# # hotel-booking dataset distinct values: 

# distinct_Hoteltype = df_hotel.select("hotel").distinct().count()
# print("Distinct Hotel Type: ", distinct_Hoteltype)

# distinct_arrivalYEARHOTEL = df_hotel.select("arrival_year").distinct().count()
# print("Distinct Arrival Year Hotel: ", distinct_arrivalYEARHOTEL)

# distinct_arrivalMONTHHOTEL = df_hotel.select("arrival_month").distinct().count()
# print("Distinct Arrival Month Hotel: ", distinct_arrivalMONTHHOTEL)

# distinct_marketSegmentHOTEL = df_hotel.select("market_segment_type").distinct().count()
# print("Distinct Market Segment Hotel: ", distinct_marketSegmentHOTEL)

# distinct_country = df_hotel.select("country").distinct().count()
# print("Distinct Country: ", distinct_country)


# distinct_hotelRows = df_hotel.distinct().count()
# totalRows = df_hotel.count()
# print("Distinct Hotel Rows: ", distinct_hotelRows, "out of ", totalRows) 


# # checking for null values in customer-reservations dataset: 

# # https://stackoverflow.com/questions/37262762/filter-pyspark-dataframe-column-with-none-value: used this for reference 
# # https://sparkbyexamples.com/pyspark/pyspark-filter-rows-with-null-values/: used this for reference

# from pyspark.sql.functions import col, sum 

# counter_null_customer = df_customer.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_customer.columns])
# counter_null_customer.show()

# # checking for null values in hotel-booking dataset:

# counter_null_hotel = df_hotel.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_hotel.columns])
# counter_null_hotel.show()



# # analyzing distribution of numerical columns in the datasets: 
# # https://www.projectpro.io/recipes/explain-kurtosis-min-max-and-mean-aggregate-functions-pyspark-databricks#:~:text=The%20PySpark%20min%20and%20max,RDD%20(Resilient%20Distributed%20Dataset).&text=The%20PySpark%20mean%20function%20calculates%20the%20average%20value%20of%20a%20given%20dataset.: used this for reference

# # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aggregate.html

# from pyspark.sql.functions import mean, max, stddev, min 

# # customer-reservations dataset: 

# df_customer.select(
#     mean("lead_time").alias("mean_lead_time"),
#     max("lead_time").alias("max_lead_time"),
#     min("lead_time").alias("min_lead_time"),
#     stddev("lead_time").alias("stddev_lead_time")
# ).show()

# df_customer.select(   
#     mean("stays_in_week_nights").alias("mean_stays_in_week_nights"),
#     max("stays_in_week_nights").alias("max_stays_in_week_nights"),
#     min("stays_in_week_nights").alias("min_stays_in_week_nights"),
#     stddev("stays_in_week_nights").alias("stddev_stays_in_week_nights")
# ).show()

# df_customer.select(   
#     mean("stays_in_weekend_nights").alias("mean_stays_in_weekend_nights"),
#     max("stays_in_weekend_nights").alias("max_stays_in_weekend_nights"),
#     min("stays_in_weekend_nights").alias("min_stays_in_weekend_nights"),
#     stddev("stays_in_weekend_nights").alias("stddev_stays_in_weekend_nights")
# ).show()

# df_customer.select(   
#     mean("avg_price_per_room").alias("mean_avg_price_per_room"),
#     max("avg_price_per_room").alias("max_avg_price_per_room"),
#     min("avg_price_per_room").alias("min_avg_price_per_room"),
#     stddev("avg_price_per_room").alias("stddev_avg_price_per_room")
# ).show()


# # hotel -booking dataset:

# df_hotel.select(
#     mean("lead_time").alias("mean_lead_time"),
#     max("lead_time").alias("max_lead_time"),
#     min("lead_time").alias("min_lead_time"),
#     stddev("lead_time").alias("stddev_lead_time")
# ).show()
    
# df_hotel.select(  
#     mean("stays_in_week_nights").alias("mean_stays_in_week_nights"),
#     max("stays_in_week_nights").alias("max_stays_in_week_nights"),
#     min("stays_in_week_nights").alias("min_stays_in_week_nights"),
#     stddev("stays_in_week_nights").alias("stddev_stays_in_week_nights")
# ).show()
  
# df_hotel.select(  
#     mean("stays_in_weekend_nights").alias("mean_stays_in_weekend_nights"),
#     max("stays_in_weekend_nights").alias("max_stays_in_weekend_nights"),
#     min("stays_in_weekend_nights").alias("min_stays_in_weekend_nights"),
#     stddev("stays_in_weekend_nights").alias("stddev_stays_in_weekend_nights")
# ).show()

# df_hotel.select( 
#     mean("avg_price_per_room").alias("mean_avg_price_per_room"),
#     max("avg_price_per_room").alias("max_avg_price_per_room"),
#     min("avg_price_per_room").alias("min_avg_price_per_room"),
#     stddev("avg_price_per_room").alias("stddev_avg_price_per_room")
# ).show()



# # check for the duplicate row that we found the hotel-booking dataset:

# # used this for reference: https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/

# from pyspark.sql.functions import count 
# df_hotel.groupBy(df_hotel.columns).agg(count("*").alias("count")).filter(col("count") > 1).show()

# # just a check for duplicate row in customer dataset: 
# #from pyspark.sql.functions import count 
# #df_customer.groupBy(df_customer.columns).agg(count("*").alias("count")).filter(col("count") > 1).show()
# # 


# # Distinction between market_segment_type in customer_reservation and hotel booking dataset: 

# df_customer.groupBy("market_segment_type").count().show()

# df_hotel.groupBy("market_segment_type").count().show()




# # Numerical number of booking that we cancelled/ not cancelled: 

# df_customer.groupBy("Booking_Status").count().show()



# ============================== Creating Derived columns for analysis =============================
from pyspark.sql.functions import col, count, sum, avg, when, round
import matplotlib
matplotlib.use('Agg')

# Create derived columns in Spark
print("\n=== CREATING DERIVED COLUMNS ===")

# Create is_canceled column from booking_status
df_customer = df_customer.withColumn("is_canceled", 
    when(col("booking_status") == "Canceled", 1).otherwise(0))

df_hotel = df_hotel.withColumn("is_canceled", col("booking_status"))  # Already binary

# Create total_nights column
df_customer = df_customer.withColumn("total_nights", 
    col("stays_in_weekend_nights") + col("stays_in_week_nights"))

df_hotel = df_hotel.withColumn("total_nights", 
    col("stays_in_weekend_nights") + col("stays_in_week_nights"))

# Create total_booking_value column
df_customer = df_customer.withColumn("total_booking_value", 
    col("avg_price_per_room") * col("total_nights"))

df_hotel = df_hotel.withColumn("total_booking_value", 
    col("avg_price_per_room") * col("total_nights"))

# Create is_zero_price column
df_customer = df_customer.withColumn("is_zero_price", 
    when(col("avg_price_per_room") == 0, True).otherwise(False))

df_hotel = df_hotel.withColumn("is_zero_price", 
    when(col("avg_price_per_room") == 0, True).otherwise(False))

print("✓ Derived columns created successfully!")

# ==============================Custimer dataset cancellation by segment=============================
customer_cancel_by_segment = df_customer.groupBy("market_segment_type").agg(
    count("*").alias("total_bookings"), 
    sum('is_canceled').alias("canceled_bookings"),
    avg('is_canceled').alias('cancellation_rate')
).withColumn('cancellation_rate', round(col('cancellation_rate') * 100, 1))

print("Custumer dataset cancellation by segment:")
customer_cancel_by_segment.show()

# hotel dataset cancellation by segment:
hotel_cancel_by_segment = df_hotel.groupBy("market_segment_type").agg(
    count("*").alias("total_bookings"), 
    sum('is_canceled').alias("canceled_bookings"),
    avg('is_canceled').alias('cancellation_rate')
).withColumn('cancellation_rate', round(col('cancellation_rate') * 100, 1))

print("Hotel dataset cancellation by segment:")
hotel_cancel_by_segment.show()

# Create a simple bar chart for cancellation rates
import matplotlib.pyplot as plt
import numpy as np

# Data from your analysis
segments = ['Groups', 'Online TA', 'Offline TA/TO', 'Corporate', 'Aviation', 'Direct', 'Complementary']
cancel_rates = [62.3, 33.9, 34.5, 15.9, 22.8, 15.3, 11.7]

plt.figure(figsize=(12, 6))
bars = plt.bar(segments, cancel_rates, color=['red', 'orange', 'orange', 'green', 'yellow', 'green', 'green'])
plt.title('Cancellation Rate by Market Segment', fontsize=16, fontweight='bold')
plt.ylabel('Cancellation Rate (%)', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.ylim(0, 70)

# Color code: Red (>50%), Orange (30-50%), Green (<30%)
for bar, rate in zip(bars, cancel_rates):
    if rate > 50:
        bar.set_color('red')
    elif rate > 30:
        bar.set_color('orange')
    else:
        bar.set_color('green')

plt.tight_layout()
plt.savefig('../reports/figures/cancellation_by_segment.png', dpi=300, bbox_inches='tight')
plt.show()

# ==============================Portugal vs International Analysis=============================
print('\n' + '='*80)
print("PORTUGAL VS INTERNATIONAL ANALYSIS")
print('='*80)

# Portugal analysis
portugal_stats = df_hotel.filter(col('country') == 'PRT').agg(
    count("*").alias("total_bookings"), 
    sum('is_canceled').alias("canceled_bookings"),
    avg(when(col('is_zero_price') == False, col('avg_price_per_room'))).alias('avg_price'),
    avg('lead_time').alias('avg_lead_time'), 
    avg('total_nights').alias('avg_stay'),
    avg('total_booking_value').alias('avg_booking_value')
)

print("Portugal analysis:")
portugal_stats.select(
    col('total_bookings'),
    round(col('canceled_bookings') * 100, 1).alias('canceled_bookings_%'),
    round(col('avg_price'), 2).alias('avg_price'),
    round(col('avg_lead_time'), 1).alias('avg_lead_time'),
    round(col('avg_stay'), 1).alias('avg_stay'),
    round(col('avg_booking_value'), 2).alias('avg_booking_value')
).show()

# International analysis
international_stats = df_hotel.filter(col('country') != 'PRT').agg(
    count("*").alias("total_bookings"), 
    sum('is_canceled').alias("canceled_bookings"),
    avg(when(col('is_zero_price') == False, col('avg_price_per_room'))).alias('avg_price'),
    avg('lead_time').alias('avg_lead_time'), 
    avg('total_nights').alias('avg_stay'),
    avg('total_booking_value').alias('avg_booking_value')
)

print("Other countries analysis:")
international_stats.select(
    col('total_bookings'),
    round(col('canceled_bookings') * 100, 1).alias('canceled_bookings_%'),
    round(col('avg_price'), 2).alias('avg_price'),
    round(col('avg_lead_time'), 1).alias('avg_lead_time'),
    round(col('avg_stay'), 1).alias('avg_stay'),
    round(col('avg_booking_value'), 2).alias('avg_booking_value')
).show()

# Portugal vs Others comparison
categories = ['Cancellation Rate', 'Average Price', 'Lead Time (days)', 'Booking Value']
portugal_values = [56.1, 91.34, 117.8, 274.77]
others_values = [19.8, 102.01, 88.1, 370.38]

x = np.arange(len(categories))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))
bars1 = ax.bar(x - width/2, portugal_values, width, label='Portugal', color='lightcoral')
bars2 = ax.bar(x + width/2, others_values, width, label='Other Countries', color='lightblue')

ax.set_xlabel('Metrics', fontsize=12)
ax.set_ylabel('Values', fontsize=12)
ax.set_title('Portugal vs Other Countries Comparison', fontsize=16, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(categories)
ax.legend()

# Add value labels
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{height:.1f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('../reports/figures/portugal_vs_others.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================== City Hotel vs Resort Hotel Analysis=============================
print('\n' + '='*80)
print("CITY HOTEL VS RESORT HOTEL ANALYSIS")
print('='*80)

# City hotel analysis
city_stats = df_hotel.filter(col("hotel") == "City Hotel").agg(
    count("*").alias("total_bookings"),
    avg("is_canceled").alias("cancelation_rate"),
    avg(when(col("is_zero_price") == False, col("avg_price_per_room"))).alias("avg_price"),
    avg("lead_time").alias("avg_lead_time"),
    avg("total_nights").alias("avg_stay"),
    avg("total_booking_value").alias("avg_booking_value")
)

print("CITY HOTEL Analysis:")
city_stats.select(
    col("total_bookings"),
    round(col("cancelation_rate") * 100, 1).alias("cancelation_rate_%"),
    round(col("avg_price"), 2).alias("avg_price_$"),
    round(col("avg_lead_time"), 1).alias("avg_lead_time_days"),
    round(col("avg_stay"), 1).alias("avg_stay_nights"),
    round(col("avg_booking_value"), 2).alias("avg_booking_value_$")
).show()

# Resort hotel analysis
resort_stats = df_hotel.filter(col("hotel") == "Resort Hotel").agg(
    count("*").alias("total_bookings"),
    avg("is_canceled").alias("cancelation_rate"),
    avg(when(col("is_zero_price") == False, col("avg_price_per_room"))).alias("avg_price"),
    avg("lead_time").alias("avg_lead_time"),
    avg("total_nights").alias("avg_stay"),
    avg("total_booking_value").alias("avg_booking_value")
)

print("RESORT HOTEL Analysis:")
resort_stats.select(
    col("total_bookings"),
    round(col("cancelation_rate") * 100, 1).alias("cancelation_rate_%"),
    round(col("avg_price"), 2).alias("avg_price_$"),
    round(col("avg_lead_time"), 1).alias("avg_lead_time_days"),
    round(col("avg_stay"), 1).alias("avg_stay_nights"),
    round(col("avg_booking_value"), 2).alias("avg_booking_value_$")
).show()

# ============================== Lead Time Impact on Cancellation =============================
print('\n' + '='*80)
print("LEAD TIME IMPACT ON CANCELLATION")
print('='*80)

# creating lead time categories
df_hotel = df_hotel.withColumn("lead_time_category", 
    when(col("lead_time") <= 7, "Last Minute (≤7 days)")
    .when(col("lead_time") <= 30, "Short Term (8-30 days)")
    .when(col("lead_time") <= 90, "Medium Term (31-90 days)")
    .otherwise("Long Term (>90 days)")
)

# Analyze cancellation by lead time
lead_time_analysis = df_hotel.groupBy("lead_time_category").agg(
    count("*").alias("total_bookings"),
    sum("is_canceled").alias("canceled_bookings"),
    avg("is_canceled").alias("cancelation_rate")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Cancellation Rate by Lead Time (Hotel Dataset):")
lead_time_analysis.show()

# Lead time impact visualization
lead_categories = ['Last Minute\n(≤7 days)', 'Short Term\n(8-30 days)', 'Medium Term\n(31-90 days)', 'Long Term\n(>90 days)']
cancel_rates_lead = [10.0, 26.5, 35.5, 51.8]

plt.figure(figsize=(10, 6))
bars = plt.bar(lead_categories, cancel_rates_lead, color=['green', 'yellow', 'orange', 'red'])
plt.title('Cancellation Rate by Lead Time', fontsize=16, fontweight='bold')
plt.ylabel('Cancellation Rate (%)', fontsize=12)
plt.xlabel('Booking Lead Time', fontsize=12)
plt.ylim(0, 60)

# Add value labels on bars
for bar, rate in zip(bars, cancel_rates_lead):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
             f'{rate}%', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('../reports/figures/cancellation_by_lead_time.png', dpi=300, bbox_inches='tight')
plt.show()

# ============================== Monthly Patterns =============================
print('\n' + '='*80)
print("MONTHLY PATTERNS")
print('='*80)

# Analyze monthly patterns
monthly_analysis = df_hotel.groupBy("arrival_month").agg(
    count("*").alias("total_bookings"),
    sum("is_canceled").alias("canceled_bookings"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("total_booking_value").alias("avg_booking_value"),
    avg("lead_time").alias("avg_lead_time")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Monthly Patterns (Hotel Dataset):")
monthly_analysis.show()

# Monthly patterns heatmap
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
cancel_rates_monthly = [24.8, 34.4, 30.6, 38.0, 35.0, 39.6, 37.5, 38.2, 39.2, 38.0, 31.2, 35.0]

# Create heatmap data
heatmap_data = np.array(cancel_rates_monthly).reshape(1, 12)

plt.figure(figsize=(14, 4))
im = plt.imshow(heatmap_data, cmap='RdYlGn_r', aspect='auto')
plt.colorbar(im, label='Cancellation Rate (%)')
plt.title('Monthly Cancellation Rate Heatmap', fontsize=16, fontweight='bold')
plt.xlabel('Month', fontsize=12)
plt.yticks([])
plt.xticks(range(12), months)

# Add text annotations
for i in range(12):
    plt.text(i, 0, f'{cancel_rates_monthly[i]:.1f}%', ha='center', va='center', 
             fontweight='bold', color='white' if cancel_rates_monthly[i] > 35 else 'black')

plt.tight_layout()
plt.savefig('../reports/figures/monthly_cancellation_heatmap.png', dpi=300, bbox_inches='tight')
plt.show()