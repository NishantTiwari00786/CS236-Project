import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import scipy as sp
from scipy import stats
import sklearn
import skimage
import plotly 
import pyspark as spark

df_customer_clean = pd.read_csv('/Users/simar/Documents/Fall25/CS236/CS236-Project/data/clean/customer-reservations-clean.csv')
df_hotel_clean = pd.read_csv('/Users/simar/Documents/Fall25/CS236/CS236-Project/data/clean/hotel-booking-clean.csv')

# ========== Categorial Variables Analysis ==========
# print(f"Customer columns: {df_customer_clean.columns}.tolist()")
# print(f"Hotel columns: {df_hotel_clean.columns}.tolist()")

# print(f"Customer booking status: {df_customer_clean['booking_status'].value_counts()}")
# print(f"Hotel booking status: {df_hotel_clean['booking_status'].value_counts()}")

# calculate cancellation rates
# customer_cancel_rate = df_customer_clean['is_canceled'].mean()
# hotel_cancel_rate = df_hotel_clean['is_canceled'].mean()

# print("CANCELLATION RATES")
# print(f"Customer cancellation rate: {customer_cancel_rate:.2%}")
# print(f"Hotel cancellation rate: {hotel_cancel_rate:.2%}")
# print(f"Difference: {abs(customer_cancel_rate - hotel_cancel_rate):.2%}")

# # add business context
# print("BUSINESS CONTEXT")
# customer_cancel_count = df_customer_clean['is_canceled'].sum()
# hotel_cancel_count = df_hotel_clean['is_canceled'].sum()
# print(f"Customer cancellation count: {customer_cancel_count} out of {len(df_customer_clean)} cancelled bookings")
# print(f"Hotel cancellation count: {hotel_cancel_count} out of {len(df_hotel_clean)} cancelled bookings")
# print(f"Difference: {abs(customer_cancel_count - hotel_cancel_count)}")

# market segments analysis
# customer_market_segments = df_customer_clean['market_segment_type'].value_counts(normalize=True)
# hotel_market_segments = df_hotel_clean['market_segment_type'].value_counts(normalize=True)
# print(f"Customer market segments: {customer_market_segments}")
# print(f"Hotel market segments: {hotel_market_segments}")
# print(f"Difference: {abs(customer_market_segments - hotel_market_segments)}")

# fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
# sns.barplot(x=customer_market_segments.index, y=customer_market_segments.values, ax=ax1)
# ax1.set_title('Customer Market Segments')
# ax1.set_xlabel('Market Segment')
# ax1.set_ylabel('Percentage')
# sns.barplot(x=hotel_market_segments.index, y=hotel_market_segments.values, ax=ax2)
# ax2.set_title('Hotel Market Segments')
# ax2.set_xlabel('Market Segment')
# ax2.set_ylabel('Percentage')
# plt.xticks(rotation=90)
# plt.yticks(np.arange(0, 1.1, 0.1))
# plt.tight_layout()

# # Save BEFORE showing
# fig.savefig('../reports/figures/market_segments_comparison.png', dpi=300, bbox_inches='tight')
# print("✓ Saved: reports/market_segments_comparison.png")

# # Now display
# plt.show()

# hotel types analysis 
# print("\n" + '='*60)
# print("HOTEL TYPES ANALYSIS")
# print('='*60)

# print(f"Hotel types: {df_hotel_clean['hotel'].value_counts()}")
# print(f"Hotel type proportions: {df_hotel_clean['hotel'].value_counts(normalize=True)}")

# fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
# sns.barplot(x=df_hotel_clean['hotel'].value_counts().index, y=df_hotel_clean['hotel'].value_counts().values, ax=ax1)
# ax1.set_title('Hotel Types')
# ax1.set_xlabel('Hotel Type')
# ax1.set_ylabel('Count')

# sns.barplot(x=df_hotel_clean['hotel'].value_counts(normalize=True).index, y=df_hotel_clean['hotel'].value_counts(normalize=True).values, ax=ax2)
# ax2.set_title('Hotel Type Proportions')
# ax2.set_xlabel('Hotel Type')
# ax2.set_ylabel('Proportion')
# plt.xticks(rotation=90)
# plt.yticks(np.arange(0, 1.1, 0.1))
# plt.tight_layout()

# # Save BEFORE showing
# fig.savefig('../reports/figures/hotel_types_comparison.png', dpi=300, bbox_inches='tight')
# print("✓ Saved: reports/hotel_types_comparison.png")

# # Now display
# plt.show()

# Country analysis
# print("\n" + '='*60)
# print("COUNTRY ANALYSIS")
# print('='*60)
# print(f"Country: {df_hotel_clean['country'].value_counts().head(10)}")
# print(f"Country proportions: {df_hotel_clean['country'].value_counts(normalize=True).head(10)}")

# fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
# sns.barplot(x=df_hotel_clean['country'].value_counts().head(10).index, y=df_hotel_clean['country'].value_counts().head(10).values, ax=ax1)
# ax1.set_title('Country')
# ax1.set_xlabel('Country')
# ax1.set_ylabel('Count')

# sns.barplot(x=df_hotel_clean['country'].value_counts(normalize=True).head(10).index, y=df_hotel_clean['country'].value_counts(normalize=True).head(10).values, ax=ax2)
# ax2.set_title('Country Proportions')
# ax2.set_xlabel('Country')
# ax2.set_ylabel('Proportion')
# plt.xticks(rotation=90)
# plt.yticks(np.arange(0, 1.1, 0.1))
# plt.tight_layout()

# # Save BEFORE showing
# fig.savefig('../reports/figures/country_comparison.png', dpi=300, bbox_inches='tight')
# print("✓ Saved: reports/country_comparison.png")

# # Now display
# plt.show()

# ========== Numerical Variables Analysis ==========
# Prices Analysis

print("\n" + "="*80)
print("TOTAL BOOKING VALUE ANALYSIS")
print("="*80)

# Overall booking value statistics
print("=== OVERALL BOOKING VALUES ===")
print(f"Customer total booking value:")
print(f"  Mean: ${df_customer_clean['total_booking_value'].mean():.2f}")
print(f"  Median: ${df_customer_clean['total_booking_value'].median():.2f}")
print(f"  Max: ${df_customer_clean['total_booking_value'].max():.2f}")

print(f"\nHotel total booking value:")
print(f"  Mean: ${df_hotel_clean['total_booking_value'].mean():.2f}")
print(f"  Median: ${df_hotel_clean['total_booking_value'].median():.2f}")
print(f"  Max: ${df_hotel_clean['total_booking_value'].max():.2f}")

# Categorize booking values
def categorize_booking_value(value):
    if value <= 100:
        return 'Budget (≤$100)'
    elif value <= 200:
        return 'Mid-Range ($101-200)'
    elif value <= 500:
        return 'Premium ($201-500)'
    else:
        return 'Luxury (>$500)'

df_customer_clean['value_category'] = df_customer_clean['total_booking_value'].apply(categorize_booking_value)
df_hotel_clean['value_category'] = df_hotel_clean['total_booking_value'].apply(categorize_booking_value)

print(f"\n=== BOOKING VALUE CATEGORIES ===")
print("Customer booking value distribution:")
print(df_customer_clean['value_category'].value_counts(normalize=True))

print("\nHotel booking value distribution:")
print(df_hotel_clean['value_category'].value_counts(normalize=True))

# Revenue insights
print(f"\n=== REVENUE INSIGHTS ===")
customer_total_revenue = df_customer_clean['total_booking_value'].sum()
hotel_total_revenue = df_hotel_clean['total_booking_value'].sum()

print(f"Customer dataset total revenue: ${customer_total_revenue:,.2f}")
print(f"Hotel dataset total revenue: ${hotel_total_revenue:,.2f}")
print(f"Average revenue per booking - Customer: ${customer_total_revenue/len(df_customer_clean):.2f}")
print(f"Average revenue per booking - Hotel: ${hotel_total_revenue/len(df_hotel_clean):.2f}")

# print("\n" + "="*80)
# print("STAY DURATION ANALYSIS")
# print("="*80)

# # Overall stay duration statistics
# print("=== OVERALL STAY DURATION ===")
# print(f"Customer total nights:")
# print(f"  Mean: {df_customer_clean['total_nights'].mean():.1f} nights")
# print(f"  Median: {df_customer_clean['total_nights'].median():.1f} nights")
# print(f"  Max: {df_customer_clean['total_nights'].max()} nights")

# print(f"\nHotel total nights:")
# print(f"  Mean: {df_hotel_clean['total_nights'].mean():.1f} nights")
# print(f"  Median: {df_hotel_clean['total_nights'].median():.1f} nights")
# print(f"  Max: {df_hotel_clean['total_nights'].max()} nights")

# # Categorize stay duration
# def categorize_stay_duration(nights):
#     if nights == 1:
#         return '1 Night'
#     elif nights == 2:
#         return '2 Nights'
#     elif nights == 3:
#         return '3 Nights'
#     elif nights <= 7:
#         return '4-7 Nights'
#     else:
#         return 'Long Stay (>7 nights)'

# df_customer_clean['stay_category'] = df_customer_clean['total_nights'].apply(categorize_stay_duration)
# df_hotel_clean['stay_category'] = df_hotel_clean['total_nights'].apply(categorize_stay_duration)

# print(f"\n=== STAY DURATION CATEGORIES ===")
# print("Customer stay distribution:")
# print(df_customer_clean['stay_category'].value_counts(normalize=True))

# print("\nHotel stay distribution:")
# print(df_hotel_clean['stay_category'].value_counts(normalize=True))

# # Weekend vs weekday preferences
# print(f"\n=== WEEKEND VS WEEKDAY PREFERENCES ===")
# print("Customer dataset:")
# print(f"  Weekend nights: {df_customer_clean['stays_in_weekend_nights'].mean():.1f} avg")
# print(f"  Weekday nights: {df_customer_clean['stays_in_week_nights'].mean():.1f} avg")

# print("\nHotel dataset:")
# print(f"  Weekend nights: {df_hotel_clean['stays_in_weekend_nights'].mean():.1f} avg")
# print(f"  Weekday nights: {df_hotel_clean['stays_in_week_nights'].mean():.1f} avg")

# print("\n" + "="*80)
# print("LEAD TIME ANALYSIS")
# print("="*80)

# # Overall lead time statistics
# print("=== OVERALL LEAD TIME DISTRIBUTIONS ===")
# print(f"Customer lead time:")
# print(f"  Mean: {df_customer_clean['lead_time'].mean():.1f} days")
# print(f"  Median: {df_customer_clean['lead_time'].median():.1f} days")
# print(f"  Max: {df_customer_clean['lead_time'].max()} days")

# print(f"\nHotel lead time:")
# print(f"  Mean: {df_hotel_clean['lead_time'].mean():.1f} days")
# print(f"  Median: {df_hotel_clean['lead_time'].median():.1f} days")
# print(f"  Max: {df_hotel_clean['lead_time'].max()} days")

# # Categorize lead times
# def categorize_lead_time(days):
#     if days <= 7:
#         return 'Last Minute (≤7 days)'
#     elif days <= 30:
#         return 'Short Term (8-30 days)'
#     elif days <= 90:
#         return 'Medium Term (31-90 days)'
#     else:
#         return 'Long Term (>90 days)'

# df_customer_clean['lead_time_category'] = df_customer_clean['lead_time'].apply(categorize_lead_time)
# df_hotel_clean['lead_time_category'] = df_hotel_clean['lead_time'].apply(categorize_lead_time)

# print(f"\n=== LEAD TIME CATEGORIES ===")
# print("Customer lead time distribution:")
# print(df_customer_clean['lead_time_category'].value_counts(normalize=True))

# print("\nHotel lead time distribution:")
# print(df_hotel_clean['lead_time_category'].value_counts(normalize=True))

# print("\n" + '='*80)
# print("PRICES ANALYSIS")
# print('='*80)

# # filter out zero prices 
# customer_prices = df_customer_clean[~df_customer_clean['is_zero_price']]['avg_price_per_room']
# hotel_prices = df_hotel_clean[~df_hotel_clean['is_zero_price']]['avg_price_per_room']

# print("=== OVERALL PRICE DISTRIBUTIONS ===")
# print(f"Customer dataset (non-zero prices): {customer_prices.describe()}")
# print(f"\n  Count: {len(customer_prices)}")
# print(f"  Min: ${customer_prices.min():.2f}")
# print(f"  Max: ${customer_prices.max():.2f}")
# print(f"  Mean: ${customer_prices.mean():.2f}")
# print(f"  Median: ${customer_prices.median():.2f}")
# print(f"  Std: ${customer_prices.std():.2f}")

# print(f"\nHotel dataset (non-zero prices): {hotel_prices.describe()}")
# print(f"\n  Count: {len(hotel_prices)}")
# print(f"  Min: ${hotel_prices.min():.2f}")
# print(f"  Max: ${hotel_prices.max():.2f}")
# print(f"  Mean: ${hotel_prices.mean():.2f}")
# print(f"  Median: ${hotel_prices.median():.2f}")
# print(f"  Std: ${hotel_prices.std():.2f}")

# # calculate distribution shape
# print(f"\n=== DISTRIBUTION SHAPE ===")
# print(f"Customer price skewness: {stats.skew(customer_prices):.2f}")
# print(f"Hotel price skewness: {stats.skew(hotel_prices):.2f}")

# # calculate distribution kurtosis
# print(f"\n=== DISTRIBUTION KURTOSIS ===")
# print(f"Customer price kurtosis: {stats.kurtosis(customer_prices):.2f}")
# print(f"Hotel price kurtosis: {stats.kurtosis(hotel_prices):.2f}")

# print(f"\n=== PRICE VISUALIZATIONS ===")
# fig, axes = plt.subplots(2, 3, figsize=(18, 12))

# # histograms
# axes[0, 0].hist(customer_prices, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
# axes[0, 0].set_title('Customer: Price Distribution')
# axes[0, 0].set_xlabel('Price per Room (USD)')
# axes[0, 0].set_ylabel('Frequency')

# axes[0, 1].hist(hotel_prices, bins=50, alpha=0.7, color='salmon', edgecolor='black')
# axes[0, 1].set_title('Hotel: Price Distribution')
# axes[0, 1].set_xlabel('Price per Room (USD)')
# axes[0, 1].set_ylabel('Frequency')

# # boxplots
# box_data = [customer_prices, hotel_prices]
# box_labels = ['Customer', 'Hotel']

# axes[0, 2].boxplot(box_data, labels=box_labels, widths=0.5, medianprops={'color': 'black'}, boxprops={'color': 'black'})
# axes[0, 2].set_title('Price Distribution Comparison')
# axes[0, 2].set_ylabel('Price per Room (USD)')
# axes[0, 2].set_xlabel('Dataset')

# # KDE overlay
# customer_prices.plot(kind='kde', ax=axes[1, 0], color='skyblue', title='Customer: KDE Overlay')
# hotel_prices.plot(kind='kde', ax=axes[1, 0], color='salmon', title='Hotel: KDE Overlay')
# axes[1, 0].set_title('Price Density Comparison')
# axes[1, 0].set_xlabel('Price per Room (USD)')
# axes[1, 0].set_ylabel('Density')
# axes[1, 0].legend(['Customer', 'Hotel'])

# # price by hotel type (Hotel Only)
# hotel_city_prices = df_hotel_clean[(~df_hotel_clean['is_zero_price']) & (df_hotel_clean['hotel'] == 'City Hotel')]['avg_price_per_room']
# hotel_resort_prices = df_hotel_clean[(~df_hotel_clean['is_zero_price']) & (df_hotel_clean['hotel'] == 'Resort Hotel')]['avg_price_per_room']

# city_resort_data = [hotel_city_prices, hotel_resort_prices]
# city_resort_labels = ['City Hotel', 'Resort Hotel']

# axes[1, 1].boxplot(city_resort_data, labels=city_resort_labels, widths=0.5, medianprops={'color': 'black'}, boxprops={'color': 'black'})
# axes[1, 1].set_title('Price Distribution Comparison by Hotel Type')
# axes[1, 1].set_ylabel('Price per Room (USD)')
# axes[1, 1].set_xlabel('Hotel Type')
# axes[1, 1].legend(['City Hotel', 'Resort Hotel'])

# # price by country (Portugal vs. Others)
# hotel_portugal_prices = df_hotel_clean[(~df_hotel_clean['is_zero_price']) & (df_hotel_clean['country'] == 'Portugal')]['avg_price_per_room']
# hotel_others_prices = df_hotel_clean[(~df_hotel_clean['is_zero_price']) & (df_hotel_clean['country'] != 'Portugal')]['avg_price_per_room']

# portugal_others_data = [hotel_portugal_prices, hotel_others_prices]
# portugal_others_labels = ['Portugal', 'Others']

# axes[1, 2].boxplot(portugal_others_data, labels=portugal_others_labels, widths=0.5, medianprops={'color': 'black'}, boxprops={'color': 'black'})
# axes[1, 2].set_title('Price Distribution Comparison by Country')
# axes[1, 2].set_ylabel('Price per Room (USD)')
# axes[1, 2].set_xlabel('Country')
# axes[1, 2].legend(['Portugal', 'Others'])

# plt.tight_layout()
# plt.savefig('../reports/figures/comprehensive_price_analysis.png', dpi=300, bbox_inches='tight')
# print("✓ Saved: reports/figures/comprehensive_price_analysis.png")
# plt.show()

# print("\n=== SEGMENTED PRICE ANALYSIS ===")
# print(f"City Hotel prices: Mean=${hotel_city_prices.mean():.2f}, Median=${hotel_city_prices.median():.2f}")
# print(f"Resort Hotel prices: Mean=${hotel_resort_prices.mean():.2f}, Median=${hotel_resort_prices.median():.2f}")
# print(f"Portugal prices: Mean=${hotel_portugal_prices.mean():.2f}, Median=${hotel_portugal_prices.median():.2f}")
# print(f"Other countries prices: Mean=${hotel_others_prices.mean():.2f}, Median=${hotel_others_prices.median():.2f}")

# ========== Temporal Patterns ==========

# ========== Summary Stats & Key Findings ==========
