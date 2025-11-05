from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Phase2_Analysis").getOrCreate()

df = spark.read.csv("data/unified/unified-dataset_fix.csv/unified.csv", header = True, inferSchema = True)
# df.show(5)
# df.printSchema()

# ========================== Cancellation Rate: Calculate cancellation rate for each month ==========================

# Convert booking status to lowercase and trim whitespace
df = df.withColumn("booking_status_clean", F.lower(F.trim(F.col("booking_status"))))
# Create a boolean column to indicate if the booking was cancelled
df = df.withColumn("is_cancelled", F.when(F.col("booking_status_clean") == "cancelled", 1).otherwise(0))

#df = df.withColumn("arrival_month_clean", F.trim(F.col("arrival_month")).cast("int"))


# df.select("booking_status").distinct().show()
# df.groupBy("booking_status_clean").count().show()

#df.select("arrival_month").distinct().orderBy("arrival_month").show(50)

# Count the number of bookings for each month and booking status
df.groupBy("arrival_month", "booking_status_clean").count().orderBy("arrival_month").show(30)



# Calculate the cancellation rate for each month
cancel_rate = (
    df.groupBy("arrival_month").agg(
        (F.round(F.sum("is_cancelled") / F.count("*"), 3)).alias("cancellation_rate"), F.count("*").alias("total_bookings")).orderBy("arrival_month")
)

print("Cancellation Rate by Month:")
cancel_rate.show()

# ========================== Averages: average price and average number of nights per month ==========================
# Calculate the total number of nights stayed
df = df.withColumn("total_nights", F.col("stays_in_week_nights") + F.col("stays_in_weekend_nights"))

# Calculate the average price and average number of nights per month
avg_stats = (
    df.groupBy("arrival_month")
    .agg(
        F.round(F.avg("avg_price_per_room"), 2).alias("average_price"),
        F.round(F.avg("total_nights"), 2).alias("average_nights")
    )
    .orderBy("arrival_month")
)
avg_stats.show()

# ========================== Monthly Bookings: monthly bookings by market segment type ==========================
# Calculate the number of bookings for each month and market segment type
monthly_bookings = (
    df.groupBy("arrival_month", "market_segment_type")
    .agg(F.count("*").alias("total_bookings"))
    .orderBy("arrival_month", "market_segment_type")
)
monthly_bookings.show(monthly_bookings.count())

# ========================== Seasonality: analyze seasonality of bookings ==========================
# Calculate the revenue for each month
df = df.withColumn("revenue", F.col("avg_price_per_room") * F.col("total_nights"))

# Calculate the average revenue and total revenue per month
revenue_by_month = (
    df.groupBy("arrival_month")
    .agg(
        F.round(F.avg("revenue"), 2).alias("average_revenue"),
        F.round(F.sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy("arrival_month")
)
revenue_by_month.show()
print(f"Most popular month for revenue: {revenue_by_month.orderBy(F.desc('total_revenue')).first()[0]}")