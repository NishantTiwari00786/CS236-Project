import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import scipy as sp
import sklearn
import skimage
import plotly 
import pyspark as spark


# Load the data
df_customer = pd.read_csv('/Users/simar/Documents/Fall25/CS236/CS236-Project/data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv')
df_hotel = pd.read_csv('/Users/simar/Documents/Fall25/CS236/CS236-Project/data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv')

print(f"Customer:\n {df_customer[df_customer['avg_price_per_room'] ==0]['market_segment_type'].value_counts()}")
print(f"Hotel:\n {df_hotel[df_hotel['avg_price_per_room'] ==0]['market_segment_type'].value_counts()}")

# Price distribution
print(f"Customer price distribution: \n {df_customer['avg_price_per_room'].describe()}")
print(f"Hotel price distribution: \n {df_hotel['avg_price_per_room'].describe()}")

# Price distribution by market segment
print(f"Customer price distribution by market segment: \n {df_customer.groupby('market_segment_type')['avg_price_per_room'].describe()}")
print(f"Hotel price distribution by market segment: \n {df_hotel.groupby('market_segment_type')['avg_price_per_room'].describe()}")

# ============================================================================
# DATA CLEANING - OPTION A (Conservative Approach)
# ============================================================================

print("\n" + "="*80)
print("STARTING DATA CLEANING PROCESS")
print("="*80)

# 1. INVESTIGATE THE $5400 OUTLIER
# Why: This is 10x higher than the customer dataset max and could be a data error
print("\n--- Investigating $5400 outlier ---")
outlier_threshold = 1000  # Any price above $1000 is suspicious
hotel_outliers = df_hotel[df_hotel['avg_price_per_room'] > outlier_threshold]
print(f"Found {len(hotel_outliers)} bookings with price > ${outlier_threshold}")
print(hotel_outliers[['hotel', 'avg_price_per_room', 'stays_in_weekend_nights', 
                       'stays_in_week_nights', 'market_segment_type', 'booking_status']])

# Calculate total stay nights to see if it's a nightly rate issue
hotel_outliers_analysis = hotel_outliers.copy()
hotel_outliers_analysis['total_nights'] = hotel_outliers_analysis['stays_in_weekend_nights'] + hotel_outliers_analysis['stays_in_week_nights']
hotel_outliers_analysis['price_per_night'] = hotel_outliers_analysis['avg_price_per_room'] / hotel_outliers_analysis['total_nights'].replace(0, 1)
print("\nOutlier analysis with calculated per-night rates:")
print(hotel_outliers_analysis[['avg_price_per_room', 'total_nights', 'price_per_night']])

# 2. REMOVE DUPLICATE FROM HOTEL DATASET
# Why: The duplicate row (Lisa_M@gmail.com) is a true duplicate with identical values
print("\n--- Removing duplicates ---")
print(f"Hotel dataset before: {len(df_hotel)} rows")
df_hotel_clean = df_hotel.drop_duplicates().copy()
print(f"Hotel dataset after: {len(df_hotel_clean)} rows (removed {len(df_hotel) - len(df_hotel_clean)} duplicate)")

# Customer dataset (no duplicates, but we'll create consistent naming)
df_customer_clean = df_customer.copy()

# 3. HANDLE EXTREME OUTLIERS
# Decision: Cap prices at 99th percentile to remove data errors while keeping legitimate high prices
# Why: $5400 is likely a data entry error (missing decimal or total cost vs nightly rate)
customer_99th = df_customer_clean['avg_price_per_room'].quantile(0.99)
hotel_99th = df_hotel_clean['avg_price_per_room'].quantile(0.99)

print(f"\n--- Handling extreme outliers ---")
print(f"Customer 99th percentile price: ${customer_99th:.2f}")
print(f"Hotel 99th percentile price: ${hotel_99th:.2f}")

# Count how many will be affected
customer_outliers = (df_customer_clean['avg_price_per_room'] > customer_99th).sum()
hotel_outliers_count = (df_hotel_clean['avg_price_per_room'] > hotel_99th).sum()
print(f"Customer bookings above 99th percentile: {customer_outliers}")
print(f"Hotel bookings above 99th percentile: {hotel_outliers_count}")

# Cap the outliers
df_customer_clean.loc[df_customer_clean['avg_price_per_room'] > customer_99th, 'avg_price_per_room'] = customer_99th
df_hotel_clean.loc[df_hotel_clean['avg_price_per_room'] > hotel_99th, 'avg_price_per_room'] = hotel_99th
print("✓ Outliers capped at 99th percentile")

# 4. HANDLE ZERO PRICES
# Decision: FLAG but DON'T remove zeros
# Why: Many zeros are legitimate (Complementary bookings), but we want to track them
# We'll add a flag column for easy filtering in analysis
print(f"\n--- Flagging zero prices ---")
df_customer_clean['is_zero_price'] = (df_customer_clean['avg_price_per_room'] == 0)
df_hotel_clean['is_zero_price'] = (df_hotel_clean['avg_price_per_room'] == 0)
print(f"Customer zero prices flagged: {df_customer_clean['is_zero_price'].sum()}")
print(f"Hotel zero prices flagged: {df_hotel_clean['is_zero_price'].sum()}")
print("✓ Zero prices flagged but retained for analysis")

# 5. ADD USEFUL DERIVED FEATURES
# Why: These are commonly needed metrics that we'll use throughout EDA
print(f"\n--- Creating derived features ---")

# Total nights stayed
df_customer_clean['total_nights'] = df_customer_clean['stays_in_weekend_nights'] + df_customer_clean['stays_in_week_nights']
df_hotel_clean['total_nights'] = df_hotel_clean['stays_in_weekend_nights'] + df_hotel_clean['stays_in_week_nights']

# Total booking value (price × nights) - for non-zero prices only
df_customer_clean['total_booking_value'] = df_customer_clean['avg_price_per_room'] * df_customer_clean['total_nights']
df_hotel_clean['total_booking_value'] = df_hotel_clean['avg_price_per_room'] * df_hotel_clean['total_nights']

# Binary cancellation flag (normalized across datasets)
df_customer_clean['is_canceled'] = (df_customer_clean['booking_status'] == 'Canceled').astype(int)
df_hotel_clean['is_canceled'] = df_hotel_clean['booking_status']  # Already binary

print("✓ Added: total_nights, total_booking_value, is_canceled")

# 6. FINAL VALIDATION
print(f"\n--- Final cleaned dataset summary ---")
print(f"Customer dataset: {len(df_customer_clean)} rows, {len(df_customer_clean.columns)} columns")
print(f"Hotel dataset: {len(df_hotel_clean)} rows, {len(df_hotel_clean.columns)} columns")
print(f"\nNew columns added: {[col for col in df_customer_clean.columns if col not in df_customer.columns]}")

# 7. SAVE CLEANED DATA
print(f"\n--- Saving cleaned datasets ---")
customer_clean_path = '/Users/simar/Documents/Fall25/CS236/CS236-Project/data/clean/customer-reservations-clean.csv'
hotel_clean_path = '/Users/simar/Documents/Fall25/CS236/CS236-Project/data/clean/hotel-booking-clean.csv'

df_customer_clean.to_csv(customer_clean_path, index=False)
df_hotel_clean.to_csv(hotel_clean_path, index=False)

print(f"✓ Saved: {customer_clean_path}")
print(f"✓ Saved: {hotel_clean_path}")

print("\n" + "="*80)
print("DATA CLEANING COMPLETE - Ready for Phase 2 Analysis!")
print("="*80)
