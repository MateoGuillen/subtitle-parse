import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.preprocessing import LabelEncoder, StandardScaler
import warnings
warnings.filterwarnings('ignore')

# Load sample
df = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\sample_50k.csv")
print(f"Loaded sample: {df.shape}")

# ========== DROP CONSTANT & ID COLUMNS ==========
# depth and word_count are constant - drop
# document_id and nro_licitacion are high-cardinality IDs - drop
cols_drop = ['depth', 'word_count', 'document_id', 'nro_licitacion']
df.drop(columns=cols_drop, inplace=True, errors='ignore')
print(f"Dropped constant/ID columns: {cols_drop}")

# Identified: content_length & content_length_clean are identical
print(f"\ncontent_length == content_length_clean: {(df['content_length'] == df['content_length_clean']).all()}")

# ========== FEATURE ENGINEERING ==========
# line_end == -1 could be "end of document" marker -> create indicator
df['line_end_missing'] = (df['line_end'] == -1).astype(int)
df['line_end'] = df['line_end'].replace(-1, np.nan)

# line_span = line_end - line_start (section length in lines)
df['line_span'] = df['line_end'] - df['line_start']

# ratios
df['lines_per_page'] = df['line_span'] / (df['page'] + 1)
df['tokens_per_line'] = df['estimated_tokens'] / (df['line_span'].clip(1))
df['bytes_per_token'] = df['size_bytes'] / (df['estimated_tokens'].clip(1))
df['content_density'] = df['content_length'] / (df['estimated_tokens'].clip(1))

# Encode title as categorical
le_title = LabelEncoder()
df['title_encoded'] = le_title.fit_transform(df['title'].fillna('UNKNOWN'))

# Encode category_id
le_cat = LabelEncoder()
df['category_encoded'] = le_cat.fit_transform(df['category_id'].fillna('UNKNOWN'))

print(f"Features after engineering: {list(df.columns)}")

# ========== NUMERIC FEATURES LIST ==========
num_features = ['year', 'page', 'line_start', 'line_end', 
                'content_length', 'estimated_tokens', 'size_bytes', 
                'content_length_clean', 'line_span', 'lines_per_page',
                'tokens_per_line', 'bytes_per_token', 'content_density',
                'line_end_missing', 'title_encoded', 'category_encoded']

# Drop inf/nan
df.replace([np.inf, -np.inf], np.nan, inplace=True)
for col in num_features:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# =====================================================================
# 2.1 UNIVARIATE OUTLIER POTENTIAL
# =====================================================================
print("\n========== 2.1 UNIVARIATE OUTLIER POTENTIAL ==========")
results_univ = []
for col in num_features:
    vals = df[col].dropna().values
    if len(vals) < 10:
        continue
    skew = stats.skew(vals)
    kurt = stats.kurtosis(vals, fisher=True)
    Q1 = np.percentile(vals, 25)
    Q3 = np.percentile(vals, 75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    pct_outliers = np.mean((vals < lower) | (vals > upper)) * 100
    results_univ.append({
        'feature': col,
        'skewness': round(skew, 3),
        'kurtosis': round(kurt, 3),
        'pct_outliers_tukey': round(pct_outliers, 2),
        'outlier_score': min(1.0, pct_outliers / 30.0)  # normalized 0-1
    })

df_univ = pd.DataFrame(results_univ)
df_univ.sort_values('pct_outliers_tukey', ascending=False, inplace=True)
print(df_univ.to_string(index=False))

# =====================================================================
# 2.2 MULTICOLLINEARITY REDUCTION
# =====================================================================
print("\n========== 2.2 MULTICOLLINEARITY REDUCTION ==========")
corr_df = df[num_features].corr(method='pearson')

# Find highly correlated pairs
high_corr = []
for i in range(len(corr_df.columns)):
    for j in range(i+1, len(corr_df.columns)):
        val = corr_df.iloc[i, j]
        if abs(val) > 0.85:
            high_corr.append({
                'feature1': corr_df.columns[i],
                'feature2': corr_df.columns[j],
                'correlation': round(val, 3)
            })

df_high_corr = pd.DataFrame(high_corr)
if len(df_high_corr) > 0:
    df_high_corr.sort_values('correlation', ascending=False, inplace=True)
    print("Highly correlated pairs (|r| > 0.85):")
    print(df_high_corr.to_string(index=False))
else:
    print("No highly correlated pairs found")

# Print full correlation matrix
print("\nFull correlation matrix:")
print(corr_df.round(2).to_string())

# Calculate VIF-like metric: 1/(1-R2) with each feature vs others
from sklearn.linear_model import LinearRegression
vif_data = []
X_corr = df[num_features].dropna()
for col in num_features:
    y = X_corr[col]
    X_others = X_corr.drop(columns=[col])
    if X_others.shape[1] == 0:
        continue
    model = LinearRegression().fit(X_others, y)
    r2 = model.score(X_others, y)
    vif = 1 / (1 - r2) if r2 < 1 else 999
    vif_data.append({'feature': col, 'R2': round(r2, 4), 'VIF': round(vif, 2)})

df_vif = pd.DataFrame(vif_data)
df_vif.sort_values('VIF', ascending=False, inplace=True)
print("\nVIF analysis:")
print(df_vif.to_string(index=False))

# =====================================================================
# 2.3 DATA QUALITY & CARDINALITY
# =====================================================================
print("\n========== 2.3 DATA QUALITY & CARDINALITY ==========")
quality_data = []
for col in df.columns:
    null_pct = df[col].isna().mean() * 100
    n_unique = df[col].nunique()
    dtype = str(df[col].dtype)
    
    # Detect if it's likely an ID
    is_id = col in ['document_id', 'nro_licitacion'] or (n_unique > 0.9 * len(df) and 'text' in dtype)
    
    quality_data.append({
        'feature': col,
        'dtype': dtype,
        'null_pct': round(null_pct, 2),
        'n_unique': n_unique,
        'unique_ratio': round(n_unique / len(df), 4),
        'is_constant': n_unique <= 1,
        'is_id_candidate': is_id
    })

df_qual = pd.DataFrame(quality_data)
print(df_qual.to_string(index=False))

# =====================================================================
# 2.4 DOMAIN RELEVANCE (for pliegos/contract sections)
# =====================================================================
print("\n========== 2.4 DOMAIN RELEVANCE ==========")
domain_notes = {
    'page': 'Page number - anomalous section placement may indicate document manipulation',
    'line_start': 'Starting line - unusual positioning could signal altered content',
    'line_end': 'Ending line - section boundaries may reveal structural anomalies',
    'line_end_missing': 'Missing line_end (-1) - truncated or incomplete sections',
    'content_length': 'Section length in lines - unusually long/short sections are suspicious',
    'estimated_tokens': 'Token count - text volume anomalies in contract sections',
    'size_bytes': 'File size bytes - data size discrepancies',
    'content_length_clean': 'Clean content length - processing anomalies',
    'line_span': 'Line span of section - structural consistency check',
    'lines_per_page': 'Line density per page - unusual packing could indicate tampering',
    'tokens_per_line': 'Token density per line - text compression anomalies',
    'bytes_per_token': 'Byte efficiency - encoding or content anomalies',
    'content_density': 'Content-to-token ratio - text density irregularities',
    'year': 'Year - temporal trends or outdated templates',
    'title_encoded': 'Section title - unusual or rare section types',
    'category_encoded': 'Category - unusual procurement categories'
}

domain_scores = {
    'page': 0.6, 'line_start': 0.4, 'line_end': 0.5, 'line_end_missing': 0.8,
    'content_length': 0.7, 'estimated_tokens': 0.7, 'size_bytes': 0.5,
    'content_length_clean': 0.6, 'line_span': 0.5, 'lines_per_page': 0.4,
    'tokens_per_line': 0.3, 'bytes_per_token': 0.3, 'content_density': 0.3,
    'year': 0.4, 'title_encoded': 0.7, 'category_encoded': 0.4
}

for f in num_features:
    print(f"  {f:25s} score={domain_scores.get(f, 0.3):.1f}  - {domain_notes.get(f, '')}")

# =====================================================================
# 2.5 RANDOM FOREST PROXY IMPORTANCE
# =====================================================================
print("\n========== 2.5 RANDOM FOREST PROXY ==========")

# Prepare data for RF - use synthetic anomaly detection task
np.random.seed(42)
X_rf = df[num_features].copy()

# Drop rows with any NaN
mask = ~X_rf.isna().any(axis=1)
X_clean = X_rf[mask].copy()
idx = X_clean.index

# Create labels: 1 = original, 0 = synthetic noise (5%)
n_noise = int(len(X_clean) * 0.05)
noise_idx = np.random.choice(X_clean.index, n_noise, replace=False)

y = pd.Series(1, index=X_clean.index)
X_noise = X_clean.loc[noise_idx].copy()

# Add noise: perturb each column slightly then some columns heavily
for col in X_noise.columns:
    noise = np.random.randn(len(X_noise)) * X_clean[col].std() * 0.1
    X_noise[col] = X_noise[col] + noise
    # Heavy perturbation on some features
    heavy_cols = ['content_length', 'estimated_tokens', 'size_bytes', 'line_span', 'page', 'content_length_clean', 'content_density']
    if col in heavy_cols:
        heavy_noise = np.random.randn(len(X_noise)) * X_clean[col].std() * 2.0
        X_noise[col] = X_noise[col] + heavy_noise

X_noise[col] = X_noise[col].clip(lower=0)

# Combine
X_combined = pd.concat([X_clean, X_noise], axis=0)
y_combined = pd.concat([y, pd.Series(0, index=noise_idx)])

# Train RF
print(f"Training RF on {len(X_combined)} samples ({y_combined.sum()} original, {(1-y_combined).sum()} noise)...")
rf = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42, n_jobs=-1)
rf.fit(X_combined, y_combined)

rf_importances = pd.DataFrame({
    'feature': X_combined.columns,
    'rf_importance': rf.feature_importances_
})
rf_importances.sort_values('rf_importance', ascending=False, inplace=True)
rf_importances['rf_importance_norm'] = rf_importances['rf_importance'] / rf_importances['rf_importance'].max()
print("\nRandom Forest feature importances:")
print(rf_importances.to_string(index=False))

# =====================================================================
# 2.6 ISOLATION FOREST DIRECT ANALYSIS
# =====================================================================
print("\n========== 2.6 ISOLATION FOREST ANALYSIS ==========")

# Use clean data for IF
iso_X = X_clean.copy()
scaler = StandardScaler()
iso_X_scaled = pd.DataFrame(scaler.fit_transform(iso_X), columns=iso_X.columns, index=iso_X.index)

# Use a subset if too large
if len(iso_X_scaled) > 10000:
    np.random.seed(42)
    sub_idx = np.random.choice(iso_X_scaled.index, 10000, replace=False)
    iso_sub = iso_X_scaled.loc[sub_idx]
else:
    iso_sub = iso_X_scaled

print(f"Training Isolation Forest on {len(iso_sub)} samples...")
iso = IsolationForest(contamination=0.05, random_state=42, n_estimators=200)
iso.fit(iso_sub)

# Permutation importance for IF
print("Computing permutation importance for Isolation Forest...")

def if_anomaly_score(est, X, y=None):
    return -est.score_samples(X).mean()

result = permutation_importance(
    iso, iso_sub, None,
    scoring=if_anomaly_score,
    n_repeats=5, random_state=42, n_jobs=-1
)

perm_imp = pd.DataFrame({
    'feature': iso_sub.columns,
    'if_importance_mean': result.importances_mean,
    'if_importance_std': result.importances_std
})
perm_imp.sort_values('if_importance_mean', ascending=False, inplace=True)

# Normalize
imp_min = perm_imp['if_importance_mean'].min()
imp_max = perm_imp['if_importance_mean'].max()
if imp_max > imp_min:
    perm_imp['if_importance_norm'] = (perm_imp['if_importance_mean'] - imp_min) / (imp_max - imp_min)
else:
    perm_imp['if_importance_norm'] = 0

print("\nIsolation Forest permutation importance:")
print(perm_imp.to_string(index=False))

# =====================================================================
# 2.7 CONTEXTUAL ANOMALY ANALYSIS
# =====================================================================
print("\n========== 2.7 CONTEXTUAL ANOMALY ANALYSIS ==========")

# Group by title to find contextual outliers
print("\nContextual analysis by section title:")
contextual_results = []
for col in ['content_length', 'estimated_tokens', 'size_bytes', 'line_span']:
    for group in ['title_encoded', 'category_encoded']:
        grp_stats = df.groupby(group)[col].agg(['mean', 'std', 'count'])
        grp_stats = grp_stats[grp_stats['count'] > 10]  # min samples
        merged = df[[group, col]].merge(grp_stats, on=group, how='left')
        if merged['std'].std() > 0:
            merged['z_score'] = (merged[col] - merged['mean']) / merged['std'].clip(1)
            pct_contextual = (merged['z_score'].abs() > 3).mean() * 100
        else:
            pct_contextual = 0
        contextual_results.append({
            'feature': col,
            'group_by': group,
            'pct_contextual_outliers': round(pct_contextual, 2)
        })

df_ctx = pd.DataFrame(contextual_results)
print(df_ctx.to_string(index=False))

# =====================================================================
# 3. FINAL RANKING
# =====================================================================
print("\n\n========== 3. FINAL RANKING ==========")

# Build unified ranking table
ranking = pd.DataFrame({'feature': num_features})

# Merge all scores
ranking = ranking.merge(df_univ[['feature', 'outlier_score']], on='feature', how='left')
ranking = ranking.merge(rf_importances[['feature', 'rf_importance_norm']], on='feature', how='left')
ranking = ranking.merge(perm_imp[['feature', 'if_importance_norm']], on='feature', how='left')
ranking['domain_score'] = ranking['feature'].map(domain_scores)

# Penalty: high VIF (>10) or high nulls
vif_penalty = dict(zip(df_vif['feature'], df_vif['VIF']))
ranking['vif_penalty'] = ranking['feature'].map(lambda x: min(0.1, max(0, (vif_penalty.get(x, 1) - 10) / 100)))
null_penalty = dict(zip(df_qual['feature'], df_qual['null_pct']))
ranking['null_penalty'] = ranking['feature'].map(lambda x: min(0.1, null_penalty.get(x, 0) / 100))

# Fill NaN scores
ranking.fillna({
    'outlier_score': 0,
    'rf_importance_norm': 0,
    'if_importance_norm': 0,
    'domain_score': 0.3,
    'vif_penalty': 0,
    'null_penalty': 0
}, inplace=True)

# Weighted formula
ranking['final_score'] = (
    0.25 * ranking['outlier_score'] +
    0.30 * ranking['domain_score'] +
    0.25 * ranking['rf_importance_norm'] +
    0.10 * ranking['if_importance_norm'] -
    0.05 * ranking['vif_penalty'] -
    0.05 * ranking['null_penalty']
)

ranking.sort_values('final_score', ascending=False, inplace=True)
ranking['rank'] = range(1, len(ranking) + 1)

print("\n=== FINAL FEATURE RANKING ===")
cols_show = ['rank', 'feature', 'final_score', 'outlier_score', 'domain_score', 
             'rf_importance_norm', 'if_importance_norm', 'vif_penalty', 'null_penalty']
print(ranking[cols_show].round(4).to_string(index=False))

# Save results
ranking.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\final_ranking.csv", index=False)
print("\nResults saved!")

# Also save all supporting tables
df_univ.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\univariate_results.csv", index=False)
df_high_corr.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\high_corr.csv", index=False)
df_vif.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\vif_results.csv", index=False)
rf_importances.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\rf_importances.csv", index=False)
perm_imp.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\if_importances.csv", index=False)
