from pyspark.sql import SparkSession #importing spark session 

spark = SparkSession.builder.appName("LoadingDataSet").getOrCreate() # generating spark session



# Loading both the datasets

df_customer = spark.read.csv("../data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("../data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)


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

print("✓ Derived columns created successfully!")



# ===========================================================
# ==================== Bivariate Analysis ====================
# ===========================================================

# ============================== Price vs Cancellation Rate =============================
print('\n' + '='*80)
print("PRICE VS CANCELLATION RATE")
print('='*80)

# Analyze cancellation rates by price ranges
df_hotel = df_hotel.withColumn("price_category", 
    when(col("avg_price_per_room") <= 50, "Budget ($0-50)")
    .when(col("avg_price_per_room") <= 100, "Mid ($51-100)")
    .when(col("avg_price_per_room") <= 150, "Premium ($101-150)")
    .otherwise("Luxury (>$150)"))

price_cancel_analysis = df_hotel.groupBy("price_category").agg(
    count("*").alias("total_bookings"),
    sum("is_canceled").alias("canceled_bookings"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("avg_price_per_room").alias("avg_price"),
    avg("total_booking_value").alias("avg_booking_value")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Cancellation by Price Category:")
price_cancel_analysis.orderBy("avg_price").show()

# price vs cancellation bar chart
price_categories = ['Budget\n($0-50)', 'Mid\n($51-100)', 'Premium\n($101-150)', 'Luxury\n(>$150)']
cancel_rates = [21.1, 39.1, 37.9, 33.9]
avg_prices = [32.3, 76.5, 120.6, 187.4]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# cancellation rates
bars1 = ax1.bar(price_categories, cancel_rates, color=['green', 'red', 'orange', 'orange'])
ax1.set_title('Cancellation Rate by Price Category', fontsize=14, fontweight='bold')
ax1.set_ylabel('Cancellation Rate (%)')
ax1.set_ylim(0, 45)

# add value labels
for bar, rate in zip(bars1, cancel_rates):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
             f'{rate}%', ha='center', va='bottom', fontweight='bold')

# average prices
bars2 = ax2.bar(price_categories, avg_prices, color=['lightgreen', 'lightcoral', 'lightsalmon', 'gold'])
ax2.set_title('Average Price by Category', fontsize=14, fontweight='bold')
ax2.set_ylabel('Average Price ($)')

# add value labels
for bar, price in zip(bars2, avg_prices):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2, 
             f'${price:.0f}', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('../reports/figures/bivariate/price_vs_cancellation.png', dpi=300, bbox_inches='tight')
print("✓ Saved: bivariate/price_vs_cancellation.png")
plt.show()

# ============================== Lead Time vs Cancellation Rate =============================
print('\n' + '='*80)
print("LEAD TIME VS CANCELLATION RATE")
print('='*80)

# create lead time categories
df_hotel = df_hotel.withColumn("lead_time_category", 
    when(col("lead_time") <= 7, "Last Minute (≤7 days)")
    .when(col("lead_time") <= 30, "Short Term (8-30 days)")
    .when(col("lead_time") <= 90, "Medium Term (31-90 days)")
    .otherwise("Long Term (>90 days)"))

lead_time_price_analysis = df_hotel.groupBy("lead_time_category").agg(
    count("*").alias("total_bookings"),
    avg("avg_price_per_room").alias("avg_price"),
    avg("lead_time").alias("avg_lead_time"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("total_booking_value").alias("avg_booking_value")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Lead Time vs Price Analysis:")
lead_time_price_analysis.orderBy("avg_lead_time").show()

# lead time vs cancellation scatter plot
lead_times = [2.3, 17.7, 57.1, 205.0]
cancel_rates_lead = [10.0, 26.5, 35.5, 51.8]
avg_prices_lead = [87.3, 102.9, 101.5, 91.7]
categories = ['Last Minute', 'Short Term', 'Medium Term', 'Long Term']

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

# lead time vs cancellation
ax1.scatter(lead_times, cancel_rates_lead, s=200, c=['green', 'yellow', 'orange', 'red'], alpha=0.7)
for i, cat in enumerate(categories):
    ax1.annotate(cat, (lead_times[i], cancel_rates_lead[i]), 
                xytext=(5, 5), textcoords='offset points', fontsize=10)
ax1.set_xlabel('Average Lead Time (days)')
ax1.set_ylabel('Cancellation Rate (%)')
ax1.set_title('Lead Time vs Cancellation Rate', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3)

# lead time vs price
ax2.scatter(lead_times, avg_prices_lead, s=200, c=['green', 'yellow', 'orange', 'red'], alpha=0.7)
for i, cat in enumerate(categories):
    ax2.annotate(cat, (lead_times[i], avg_prices_lead[i]), 
                xytext=(5, 5), textcoords='offset points', fontsize=10)
ax2.set_xlabel('Average Lead Time (days)')
ax2.set_ylabel('Average Price ($)')
ax2.set_title('Lead Time vs Average Price', fontsize=14, fontweight='bold')
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('../reports/figures/bivariate/lead_time_impact.png', dpi=300, bbox_inches='tight')
print("✓ Saved: bivariate/lead_time_impact.png")
plt.show()

# ============================== Stay Duration vs Market Segment =============================
print('\n' + '='*80)
print("STAY DURATION VS MARKET SEGMENT")
print('='*80)

# create stay duration categories
df_hotel = df_hotel.withColumn("stay_category", 
    when(col("total_nights") == 1, "1 Night")
    .when(col("total_nights") == 2, "2 Nights")
    .when(col("total_nights") == 3, "3 Nights")
    .when(col("total_nights") <= 7, "4-7 Nights")
    .otherwise("Long Stay (>7 nights)"))

stay_segment_analysis = df_hotel.groupBy("market_segment_type", "stay_category").agg(
    count("*").alias("bookings"),
    avg("avg_price_per_room").alias("avg_price"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("total_booking_value").alias("avg_booking_value")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Stay Duration by Market Segment (Top combinations):")
stay_segment_analysis.orderBy(col("bookings").desc()).show(20)

# ============================== Weekend vs Weekday by Country =============================
print('\n' + '='*80)
print("WEEKEND VS WEEKDAY BY COUNTRY")
print('='*80)

# getting top 10 countries
top_countries = df_hotel.groupBy("country").count().orderBy(col("count").desc()).limit(10)
top_country_list = [row["country"] for row in top_countries.collect()]

weekend_preferences = df_hotel.filter(col("country").isin(top_country_list)).groupBy("country").agg(
    count("*").alias("total_bookings"),
    avg("stays_in_weekend_nights").alias("avg_weekend_nights"),
    avg("stays_in_week_nights").alias("avg_weekday_nights"),
    avg("is_canceled").alias("cancelation_rate")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Weekend vs Weekday Preferences by Top Countries:")
weekend_preferences.orderBy(col("total_bookings").desc()).show()

# weekend vs weekday preferences by country heatmap
countries = ['PRT', 'GBR', 'FRA', 'ESP', 'DEU', 'ITA', 'IRL', 'BEL', 'NLD', 'BRA']
weekend_nights = [0.72, 1.39, 0.93, 0.86, 0.94, 0.98, 1.52, 1.04, 1.07, 0.98]
weekday_nights = [2.16, 3.55, 2.48, 2.20, 2.45, 2.20, 3.83, 2.51, 2.67, 2.41]
cancel_rates_country = [56.1, 16.1, 14.5, 21.9, 13.3, 32.1, 19.4, 16.7, 15.5, 31.8]

heatmap_data = np.array([weekend_nights, weekday_nights, cancel_rates_country])

plt.figure(figsize=(12, 6))
im = plt.imshow(heatmap_data, cmap='RdYlBu_r', aspect='auto')
plt.colorbar(im, label='Values')

plt.title('Country Preferences & Cancellation Rates', fontsize=14, fontweight='bold')
plt.xlabel('Countries')
plt.ylabel('Metrics')
plt.xticks(range(len(countries)), countries)
plt.yticks(range(3), ['Weekend Nights', 'Weekday Nights', 'Cancellation Rate (%)'])

# add text labels
for i in range(3):
    for j in range(len(countries)):
        text = f'{heatmap_data[i, j]:.1f}'
        plt.text(j, i, text, ha='center', va='center', fontweight='bold', 
                color='white' if heatmap_data[i, j] > np.mean(heatmap_data[i]) else 'black')

plt.tight_layout()
plt.savefig('../reports/figures/bivariate/country_preferences_heatmap.png', dpi=300, bbox_inches='tight')
print("✓ Saved: bivariate/country_preferences_heatmap.png")
plt.show()

# ============================== Seasonal Patterns by Hotel Type =============================
print('\n' + '='*80)
print("SEASONAL PATTERNS BY HOTEL TYPE")
print('='*80)

seasonal_hotel_analysis = df_hotel.groupBy("hotel", "arrival_month").agg(
    count("*").alias("bookings"),
    avg("avg_price_per_room").alias("avg_price"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("total_booking_value").alias("avg_booking_value")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Seasonal Patterns by Hotel Type (Top months):")
seasonal_hotel_analysis.orderBy(col("bookings").desc()).show(20)
    
# ============================== Customer Behavior Clusters =============================
print('\n' + '='*80)
print("CUSTOMER BEHAVIOR CLUSTERS")
print('='*80)

# creating customer behavior clusters
df_hotel = df_hotel.withColumn("customer_type", 
    when((col("lead_time") <= 7) & (col("total_nights") <= 2), "Last-Minute Short")
    .when((col("lead_time") > 90) & (col("total_nights") > 7), "Planner Long-Stay")
    .when((col("lead_time") <= 30) & (col("total_nights") <= 3), "Quick Getaway")
    .when(col("lead_time") > 180, "Ultra Planner")
    .otherwise("Regular Traveler"))

behavior_clusters = df_hotel.groupBy("customer_type").agg(
    count("*").alias("total_bookings"),
    avg("avg_price_per_room").alias("avg_price"),
    avg("lead_time").alias("avg_lead_time"),
    avg("total_nights").alias("avg_stay"),
    avg("is_canceled").alias("cancelation_rate"),
    avg("total_booking_value").alias("avg_booking_value")
).withColumn("cancelation_rate", round(col("cancelation_rate") * 100, 1))

print("Customer Behavior Clusters:")
behavior_clusters.orderBy(col("total_bookings").desc()).show()

# customer behavior clusters
customer_types = ['Ultra Planner', 'Regular Traveler', 'Quick Getaway', 'Last-Minute Short', 'Planner Long-Stay']
bookings = [15340, 40531, 10479, 10271, 2082]
cancel_rates_clusters = [61.5, 36.8, 24.0, 9.0, 31.9]
avg_values = [271.2, 391.6, 220.6, 110.2, 1062.0]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# bookings by customer type
bars1 = ax1.barh(customer_types, bookings, color=['red', 'orange', 'yellow', 'green', 'blue'])
ax1.set_xlabel('Number of Bookings')
ax1.set_title('Bookings by Customer Type', fontsize=14, fontweight='bold')

# add value labels
for i, (bar, booking) in enumerate(zip(bars1, bookings)):
    ax1.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2, 
             f'{booking:,}', ha='left', va='center', fontweight='bold')

# cancellation rates by customer type
bars2 = ax2.barh(customer_types, cancel_rates_clusters, color=['red', 'orange', 'yellow', 'green', 'blue'])
ax2.set_xlabel('Cancellation Rate (%)')
ax2.set_title('Cancellation Rate by Customer Type', fontsize=14, fontweight='bold')
ax2.set_xlim(0, 70)

# add value labels
for i, (bar, rate) in enumerate(zip(bars2, cancel_rates_clusters)):
    ax2.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
             f'{rate}%', ha='left', va='center', fontweight='bold')

plt.tight_layout()
plt.savefig('../reports/figures/bivariate/customer_behavior_clusters.png', dpi=300, bbox_inches='tight')
print("✓ Saved: bivariate/customer_behavior_clusters.png")
plt.show()