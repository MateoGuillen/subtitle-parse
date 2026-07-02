import pandas as pd
import numpy as np
from collections import defaultdict

# Load the CSV (saved successfully)
features = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.csv")
print(f"Loaded: {features.shape}")

# Check for duplicate columns
dup_cols = [c for c in features.columns if list(features.columns).count(c) > 1]
if dup_cols:
    print(f"Found {len(dup_cols)} duplicate column names: {dup_cols[:10]}")
    
    # Deduplicate: append _1, _2 etc to make unique
    seen = defaultdict(int)
    new_cols = []
    for c in features.columns:
        seen[c] += 1
        if seen[c] > 1:
            new_name = f"{c}_{seen[c]-1}"
            new_cols.append(new_name)
        else:
            new_cols.append(c)
    features.columns = new_cols

# Save cleaned version
features.to_parquet("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.parquet", index=False)
features.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.csv", index=False)
print(f"Saved deduplicated. Shape: {features.shape}")

print(f"\nColumn types:")
# Separate features from ID
id_col = 'nro_licitacion'
feature_cols = [c for c in features.columns if c != id_col]

# Identify numeric features
numeric_stats = []
for c in feature_cols:
    vals = features[c]
    n_unique = vals.nunique()
    nulls = vals.isna().sum()
    dtype = str(vals.dtype)
    if vals.dtype in ['int64', 'float64']:
        numeric_stats.append({
            'feature': c,
            'dtype': dtype,
            'n_unique': n_unique,
            'nulls': nulls,
            'mean': round(vals.mean(), 3) if nulls == 0 else 'NA',
            'std': round(vals.std(), 3) if nulls == 0 else 'NA',
        })

df_stats = pd.DataFrame(numeric_stats)
# Flag binary columns
df_stats['is_binary'] = df_stats['n_unique'] <= 2
df_stats['is_constant'] = df_stats['n_unique'] <= 1

print(f"\nNumeric features: {len(df_stats)}")
print(f"Binary features: {df_stats['is_binary'].sum()}")
print(f"Constant features: {df_stats['is_constant'].sum()}")

# Show first 30 features
print("\nFirst 30 features:")
print(df_stats.head(30).to_string(index=False))
