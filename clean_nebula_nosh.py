import pandas as pd
import numpy as np

def clean_data():
    print("🚀 Starting Data Cleaning Pipeline...\n")

    # --- 1. Load Data ---
    input_file = 'nebula_nosh_ops_data.csv'
    output_file = 'nebula_nosh_ops_data_cleaned.csv'
    
    try:
        df = pd.read_csv(input_file)
        print(f"✅ Loaded {input_file} with {df.shape[0]} rows and {df.shape[1]} columns.")
    except FileNotFoundError:
        print(f"❌ Error: {input_file} not found.")
        return

    # Capture initial state for summary
    initial_missing = df.isnull().sum()

    # --- 2. Standardize Text Columns ---
    print("\n🧹 Step 1: Standardizing Text Columns...")
    
    # Identify string columns
    str_cols = df.select_dtypes(include=['object']).columns
    
    for col in str_cols:
        # Strip whitespace
        df[col] = df[col].astype(str).str.strip()
        # Normalize to title case (optional, but good for consistency usually, though prompt says "appropriate")
        # For IDs like 'NN-1000' or 'KITCH-5', title case might break them if they assume caps.
        # Let's stick to strip and specific typo fixes + capitalizing simple categories if needed.
        # We will not force title case on IDs.
        
    # Fix typos in Drone_Model
    # Convert 'Swft-X' -> 'Swift-X'
    if 'Drone_Model' in df.columns:
        original_swift_counts = df['Drone_Model'].value_counts().get('Swft-X', 0)
        df['Drone_Model'] = df['Drone_Model'].replace({'Swft-X': 'Swift-X'})
        print(f"   -> Fixed {original_swift_counts} typos in 'Drone_Model' (Swft-X -> Swift-X)")

    # --- 3. Handle Invalid Numerical Values ---
    print("\n🧮 Step 2: Handling Invalid Numerical Values...")

    # A. Delivery_Time_Min 
    # Logic: < 0 or >= 300 or == 999 -> NaN
    def clean_delivery_time(val):
        if pd.isna(val) or val < 0 or val >= 300 or val == 999:
            return np.nan
        return val

    if 'Delivery_Time_Min' in df.columns:
        df['Delivery_Time_Min'] = df['Delivery_Time_Min'].apply(clean_delivery_time)
        print("   -> Masked outliers in 'Delivery_Time_Min'")
        
        # Fill NaN with median per Drone_Model
        df['Delivery_Time_Min'] = df.groupby('Drone_Model')['Delivery_Time_Min'].transform(
            lambda x: x.fillna(x.median())
        )
        print("   -> Imputed missing 'Delivery_Time_Min' using median per Drone_Model")

    # B. Order_Value_USD
    # Logic: < 0 -> NaN
    if 'Order_Value_USD' in df.columns:
        df['Order_Value_USD'] = df['Order_Value_USD'].where(df['Order_Value_USD'] >= 0, np.nan)
        print("   -> Masked negative values in 'Order_Value_USD'")
        
        # Fill NaN with median per Customer_Segment
        # Note: If Customer_Segment is missing, group by might skip it. We handle missing segments later.
        # For now, we perform transform which usually aligns with index.
        # We fill with median of segment. If segment is NaN or unique group has no val, it stays NaN.
        # We define a fallback overall median.
        overall_median_val = df['Order_Value_USD'].median()
        
        df['Order_Value_USD'] = df.groupby('Customer_Segment', dropna=False)['Order_Value_USD'].transform(
            lambda x: x.fillna(x.median() if not np.isnan(x.median()) else overall_median_val)
        )
        # Final fallback for any remaining NaNs
        df['Order_Value_USD'] = df['Order_Value_USD'].fillna(overall_median_val)
        print("   -> Imputed missing 'Order_Value_USD' using median per Customer_Segment")

    # C. Distance_KM
    # Logic: < 0 -> NaN
    if 'Distance_KM' in df.columns:
        df['Distance_KM'] = df['Distance_KM'].where(df['Distance_KM'] >= 0, np.nan)
        print("   -> Masked negative values in 'Distance_KM'")
        
        # Fill with median distance
        dist_median = df['Distance_KM'].median()
        df['Distance_KM'] = df['Distance_KM'].fillna(dist_median)
        print(f"   -> Imputed missing 'Distance_KM' with overall median ({dist_median:.2f})")

    # --- 4. Handle Missing Categorical Data ---
    print("\n🏷️ Step 3: Handling Missing Categorical Data...")

    # Customer_Segment -> 'Unknown'
    # Check for actual NaNs or string 'nan' due to strip() logic earlier
    cols_to_fix_cat = ['Customer_Segment', 'Traffic_Density', 'Customer_Rating']
    for col in cols_to_fix_cat:
        if col in df.columns:
            # Replace string 'nan' if present
            df[col] = df[col].replace('nan', np.nan)

    if 'Customer_Segment' in df.columns:
        df['Customer_Segment'] = df['Customer_Segment'].fillna('Unknown')
        # Also handle empty strings if any
        df['Customer_Segment'] = df['Customer_Segment'].replace('', 'Unknown')
        print("   -> Filled missing 'Customer_Segment' with 'Unknown'")

    if 'Traffic_Density' in df.columns:
        df['Traffic_Density'] = df['Traffic_Density'].fillna('Medium')
        df['Traffic_Density'] = df['Traffic_Density'].replace('', 'Medium')
        print("   -> Filled missing 'Traffic_Density' with 'Medium'")

    # Customer_Rating -> Fill using Median per Weather_Condition
    if 'Customer_Rating' in df.columns:
        # Ensure it's numeric
        df['Customer_Rating'] = pd.to_numeric(df['Customer_Rating'], errors='coerce')
        
        df['Customer_Rating'] = df.groupby('Weather_Condition', dropna=False)['Customer_Rating'].transform(
            lambda x: x.fillna(x.median())
        )
        # Round to nearest 0.5 or 1 if needed? Prompt implicitly suggests just filling.
        # Imputation might result in non-integer ratings (e.g. 3.5), which is acceptable.
        print("   -> Imputed missing 'Customer_Rating' using median per Weather_Condition")

    # --- 5. Data Validation Checks ---
    print("\n✅ Step 4: Final Validation...")
    
    # Assert no negative values in numeric columns (checking the ones we cleaned)
    numeric_checks = ['Delivery_Time_Min', 'Order_Value_USD', 'Distance_KM']
    for col in numeric_checks:
        if col in df.columns:
            neg_count = (df[col] < 0).sum()
            if neg_count > 0:
                print(f"   ⚠️ WARNING: Column '{col}' still has {neg_count} negative (or zero?) values.")
            else:
                print(f"   -> Assertion Passed: No negative values in '{col}'")

    # Assert Delivery Time is realistic
    outlier_delivery = ((df['Delivery_Time_Min'] >= 300) | (df['Delivery_Time_Min'] < 0)).sum()
    if outlier_delivery == 0:
        print("   -> Assertion Passed: Delivery times are within realistic range [0, 300)")
    else:
        print(f"   ⚠️ WARNING: Found {outlier_delivery} unrealistic delivery times.")

    clean_missing = df.isnull().sum()
    print("\n--- Cleaning Summary (Missing Values) ---")
    print(f"{'Column':<20} | {'Before':<10} | {'After':<10}")
    print("-" * 46)
    for col in df.columns:
        print(f"{col:<20} | {initial_missing[col]:<10} | {clean_missing[col]:<10}")

    # --- 6. Save Data ---
    df.to_csv(output_file, index=False)
    print(f"\n💾 Cleaned data saved to: {output_file}")
    print(f"📊 Final Dataset Stats: {df.shape[0]} rows, {df.shape[1]} columns")

if __name__ == "__main__":
    clean_data()
