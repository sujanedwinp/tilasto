= = = = = = = = =  Round 1: Executive Insight EDA = = = = = = = = = 


Objective: Clean the data and extract high-level business "health" metrics.


# Data Overview

- Total Records:  5,000 orders (2025).
- Key Metrics: Order Value (USD), Delivery Time (Min), Customer Ratings (1-5).
- Operational Factors: Drone Models (Swift-X, Eco-Glide, Heavy-Lift), Weather, and Traffic.

# Known Data Issues (The Cleaning Challenge)

- Typos: Inconsistent naming in `Drone_Model`.
- Financials: Negative `Order_Value_USD` entries.
- Outliers: Impossible `Delivery_Time_Min` (e.g., 999 mins).
- Missing Data: Null values in `Customer_Segment` and `Customer_Rating`.

# Executive Focus Areas

1. Revenue Trends: Which customer segments or kitchens drive the most value?
2. Efficiency: Which drone models are hitting the "35-minute" delivery goal?
3. Risk: How does weather impact refund rates and satisfaction?
