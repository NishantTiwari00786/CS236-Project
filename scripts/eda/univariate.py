from pyspark.sql import SparkSession #importing spark session 

spark = SparkSession.builder.appName("LoadingDataSet").getOrCreate() # generating spark session



# Loading both the datasets

df_customer = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)




# ============================== Creating Derived columns for analysis =============================
from pyspark.sql.functions import col, count, sum, avg, when, round
import matplotlib 
import matplotlib.pyplot as plt
import numpy as np
matplotlib.use('Agg')

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

print("Derived columns created successfully!")

# ===========================================================
# ==================== Univariate Analysis ====================
# ===========================================================

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

# bar chart for cancellation rates by segment

segments = ['Groups', 'Online TA', 'Offline TA/TO', 'Corporate', 'Aviation', 'Direct', 'Complementary']
cancel_rates = [62.3, 33.9, 34.5, 15.9, 22.8, 15.3, 11.7]

plt.figure(figsize=(12, 6))
bars = plt.bar(segments, cancel_rates, color=['red', 'orange', 'orange', 'green', 'yellow', 'green', 'green'])
plt.title('Cancellation Rate by Market Segment', fontsize=16, fontweight='bold')
plt.ylabel('Cancellation Rate (%)', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.ylim(0, 70)

# color code: Red (>50%), Orange (30-50%), Green (<30%)
for bar, rate in zip(bars, cancel_rates):
    if rate > 50:
        bar.set_color('red')
    elif rate > 30:
        bar.set_color('orange')
    else:
        bar.set_color('green')

plt.tight_layout()
plt.savefig('reports/phase1/univariate/figures/cancellation_by_segment.png', dpi=300, bbox_inches='tight')
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

# Portugal vs Others comparison bar chart
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

# add value labels
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{height:.1f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig('reports/phase1/univariate/figures/portugal_vs_others.png', dpi=300, bbox_inches='tight')
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

# Lead time impact  bar chart
lead_categories = ['Last Minute\n(≤7 days)', 'Short Term\n(8-30 days)', 'Medium Term\n(31-90 days)', 'Long Term\n(>90 days)']
cancel_rates_lead = [10.0, 26.5, 35.5, 51.8]

plt.figure(figsize=(10, 6))
bars = plt.bar(lead_categories, cancel_rates_lead, color=['green', 'yellow', 'orange', 'red'])
plt.title('Cancellation Rate by Lead Time', fontsize=16, fontweight='bold')
plt.ylabel('Cancellation Rate (%)', fontsize=12)
plt.xlabel('Booking Lead Time', fontsize=12)
plt.ylim(0, 60)

# add value labels 
for bar, rate in zip(bars, cancel_rates_lead):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
             f'{rate}%', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('reports/phase1/univariate/figures/cancellation_by_lead_time.png', dpi=300, bbox_inches='tight')
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
heatmap_data = np.array(cancel_rates_monthly).reshape(1, 12)

plt.figure(figsize=(14, 4))
im = plt.imshow(heatmap_data, cmap='RdYlGn_r', aspect='auto')
plt.colorbar(im, label='Cancellation Rate (%)')
plt.title('Monthly Cancellation Rate Heatmap', fontsize=16, fontweight='bold')
plt.xlabel('Month', fontsize=12)
plt.yticks([])
plt.xticks(range(12), months)

# add text labels
for i in range(12):
    plt.text(i, 0, f'{cancel_rates_monthly[i]:.1f}%', ha='center', va='center', 
             fontweight='bold', color='white' if cancel_rates_monthly[i] > 35 else 'black')

plt.tight_layout()
plt.savefig('reports/phase1/univariate/figures/monthly_cancellation_heatmap.png', dpi=300, bbox_inches='tight')
plt.show()