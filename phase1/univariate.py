import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import scipy as sp
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
print("\n" + '='*60)
print("HOTEL TYPES ANALYSIS")
print('='*60)

print(f"Hotel types: {df_hotel_clean['hotel'].value_counts()}")
print(f"Hotel type proportions: {df_hotel_clean['hotel'].value_counts(normalize=True)}")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
sns.barplot(x=df_hotel_clean['hotel'].value_counts().index, y=df_hotel_clean['hotel'].value_counts().values, ax=ax1)
ax1.set_title('Hotel Types')
ax1.set_xlabel('Hotel Type')
ax1.set_ylabel('Count')

sns.barplot(x=df_hotel_clean['hotel'].value_counts(normalize=True).index, y=df_hotel_clean['hotel'].value_counts(normalize=True).values, ax=ax2)
ax2.set_title('Hotel Type Proportions')
ax2.set_xlabel('Hotel Type')
ax2.set_ylabel('Proportion')
plt.xticks(rotation=90)
plt.yticks(np.arange(0, 1.1, 0.1))
plt.tight_layout()

# Save BEFORE showing
fig.savefig('../reports/figures/hotel_types_comparison.png', dpi=300, bbox_inches='tight')
print("✓ Saved: reports/hotel_types_comparison.png")

# Now display
plt.show()

# ========== Numerical Variables Analysis ==========

# ========== Temporal Patterns ==========

# ========== Summary Stats & Key Findings ==========