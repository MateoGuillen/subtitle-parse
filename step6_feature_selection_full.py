import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

# Load document-level features
df = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\features_doc_level.csv")
print(f"Loaded: {df.shape}")
id_col = 'nro_licitacion'

# Remove constant features
feature_cols = [c for c in df.columns if c != id_col]
constant_cols = [c for c in feature_cols if df[c].nunique() <= 1]
print(f"Constant features to drop: {len(constant_cols)}")
feature_cols = [c for c in feature_cols if c not in constant_cols]
print(f"Remaining features: {len(feature_cols)}")

X = df[feature_cols].copy()
# Flag any remaining NaN
if X.isna().any().any():
    print(f"NaN features: {X.isna().sum().sum()}")
    X.fillna(0, inplace=True)

print(f"\n=== 2.1 UNIVARIATE OUTLIER POTENTIAL ===")
results_univ = []
for col in feature_cols:
    vals = X[col].dropna().values
    if len(vals) < 10 or np.std(vals) == 0:
        continue
    skew = stats.skew(vals)
    kurt = stats.kurtosis(vals, fisher=True)
    Q1, Q3 = np.percentile(vals, [25, 75])
    IQR = Q3 - Q1
    if IQR == 0:
        continue
    lower, upper = Q1 - 1.5*IQR, Q3 + 1.5*IQR
    pct_out = np.mean((vals < lower) | (vals > upper)) * 100
    results_univ.append({
        'feature': col, 'skewness': round(skew, 3), 'kurtosis': round(kurt, 3),
        'pct_outliers': round(pct_out, 2),
        'outlier_score': min(1.0, pct_out / 30.0)
    })
df_univ = pd.DataFrame(results_univ)
df_univ.sort_values('pct_outliers', ascending=False, inplace=True)
print(f"Top 15 by Tukey outlier %:")
print(df_univ.head(15).to_string(index=False))

print(f"\n=== 2.2 MULTICOLLINEARITY ===")
corr = X.corr(method='pearson')
high_pairs = []
for i in range(len(corr.columns)):
    for j in range(i+1, len(corr.columns)):
        val = corr.iloc[i, j]
        if abs(val) > 0.85:
            high_pairs.append((corr.columns[i], corr.columns[j], round(val, 3)))
df_high = pd.DataFrame(high_pairs, columns=['f1', 'f2', 'corr'])
df_high.sort_values('corr', ascending=False, inplace=True)
print(f"Highly correlated pairs (|r|>0.85): {len(df_high)}")
if len(df_high) > 0:
    print(df_high.head(20).to_string(index=False))

# VIF
vif_data = []
X_vif = X.copy()
# VIF on a reduced set: drop has_* (binary) and use only numeric aggregates + len_*
vif_subset = [c for c in feature_cols if not c.startswith('has_') and not c.startswith('tok_') and X_vif[c].std() > 0]
X_vif_sub = X_vif[vif_subset]
for col in vif_subset:
    y = X_vif_sub[col]
    X_others = X_vif_sub.drop(columns=[col])
    try:
        model = LinearRegression().fit(X_others, y)
        r2 = model.score(X_others, y)
        vif = 1 / (1 - r2) if r2 < 1 else 999
        vif_data.append({'feature': col, 'R2': round(r2, 4), 'VIF': round(vif, 2)})
    except:
        vif_data.append({'feature': col, 'R2': 1.0, 'VIF': 999})
df_vif = pd.DataFrame(vif_data)
if len(df_vif) > 0:
    df_vif.sort_values('VIF', ascending=False, inplace=True)
    print(f"\nTop 15 by VIF:")
    print(df_vif.head(15).to_string(index=False))
else:
    print("No VIF data computed")

print(f"\n=== 2.3 DATA QUALITY ===")
quality = []
for col in feature_cols:
    quality.append({
        'feature': col,
        'null_pct': round(X[col].isna().mean()*100, 2),
        'n_unique': X[col].nunique(),
        'unique_ratio': round(X[col].nunique()/len(X), 4),
        'is_binary': X[col].nunique() <= 2
    })
df_qual = pd.DataFrame(quality)
print(f"Quality summary - binaries: {df_qual['is_binary'].sum()}, "
      f"nulls>0: {(df_qual['null_pct']>0).sum()}")

print(f"\n=== 2.4 DOMAIN RELEVANCE ===")
# Domain relevance scores for feature categories
def domain_score(col):
    if col.startswith('has_'):
        return 0.8  # Presence/absence of specific sections is highly informative
    elif col.startswith('len_'):
        return 0.7  # Content length of specific sections
    elif col.startswith('tok_'):
        return 0.6  # Token counts of specific sections
    elif col in ['total_sections', 'unique_titles']:
        return 0.7  # Document structure
    elif col.startswith('avg_'):
        return 0.5
    elif col.startswith('std_'):
        return 0.6  # Variance in section sizes
    elif col.startswith('max_'):
        return 0.4
    elif col.startswith('sum_'):
        return 0.4
    else:
        return 0.3

domain_scores = {col: domain_score(col) for col in feature_cols}

print(f"\n=== 2.5 RANDOM FOREST PROXY ===")
np.random.seed(42)
n_noise = int(len(X) * 0.05)
noise_idx = np.random.choice(X.index, n_noise, replace=False)

y = pd.Series(1, index=X.index)
X_noise = X.loc[noise_idx].copy()
for col in X_noise.columns:
    std_val = X[col].std()
    if std_val > 0:
        X_noise[col] = X_noise[col] + np.random.randn(n_noise) * std_val * 0.5

X_rf = pd.concat([X, X_noise], axis=0)
y_rf = pd.concat([y, pd.Series(0, index=noise_idx)])

rf = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
rf.fit(X_rf, y_rf)

rf_imp = pd.DataFrame({
    'feature': X_rf.columns,
    'rf_importance': rf.feature_importances_
})
rf_imp['rf_importance_norm'] = rf_imp['rf_importance'] / rf_imp['rf_importance'].max()
rf_imp.sort_values('rf_importance', ascending=False, inplace=True)
print("Top 20 by RF importance:")
print(rf_imp.head(20).to_string(index=False))

print(f"\n=== 2.6 ISOLATION FOREST ===")
scaler = StandardScaler()
X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns, index=X.index)

np.random.seed(42)
sub_idx = np.random.choice(X_scaled.index, min(3000, len(X_scaled)), replace=False)
X_if = X_scaled.loc[sub_idx]

iso = IsolationForest(contamination=0.05, random_state=42, n_estimators=200)
iso.fit(X_if)

def if_scorer(est, X_val, y_val=None):
    return -est.score_samples(X_val).mean()

result = permutation_importance(
    iso, X_if, None,
    scoring=if_scorer,
    n_repeats=3, random_state=42, n_jobs=-1
)
if_imp = pd.DataFrame({
    'feature': X_if.columns,
    'if_importance_mean': result.importances_mean,
    'if_importance_std': result.importances_std
})
if_imp.sort_values('if_importance_mean', ascending=False, inplace=True)
imp_min, imp_max = if_imp['if_importance_mean'].min(), if_imp['if_importance_mean'].max()
if_imp['if_importance_norm'] = (if_imp['if_importance_mean'] - imp_min) / (imp_max - imp_min) if imp_max > imp_min else 0
print("Top 20 by IF permutation importance:")
print(if_imp.head(20).to_string(index=False))

print(f"\n=== 3. FINAL RANKING ===")
ranking = pd.DataFrame({'feature': feature_cols})
ranking = ranking.merge(df_univ[['feature', 'outlier_score']], on='feature', how='left')
ranking = ranking.merge(rf_imp[['feature', 'rf_importance_norm']], on='feature', how='left')
ranking = ranking.merge(if_imp[['feature', 'if_importance_norm']], on='feature', how='left')
ranking['domain_score'] = ranking['feature'].map(domain_scores)

vif_map = dict(zip(df_vif['feature'], df_vif['VIF']))
ranking['vif_penalty'] = ranking['feature'].map(lambda x: min(0.1, max(0, (vif_map.get(x, 1) - 10) / 100)))

ranking.fillna({'outlier_score': 0, 'rf_importance_norm': 0, 'if_importance_norm': 0,
                'domain_score': 0.3, 'vif_penalty': 0}, inplace=True)

ranking['final_score'] = (
    0.25 * ranking['outlier_score'] +
    0.30 * ranking['domain_score'] +
    0.25 * ranking['rf_importance_norm'] +
    0.10 * ranking['if_importance_norm'] -
    0.10 * ranking['vif_penalty']
)

ranking.sort_values('final_score', ascending=False, inplace=True)
ranking['rank'] = range(1, len(ranking)+1)

print("\n=== TOP 20 FEATURES ===")
cols_show = ['rank', 'feature', 'final_score', 'outlier_score', 'domain_score',
             'rf_importance_norm', 'if_importance_norm']
print(ranking.head(20)[cols_show].round(4).to_string(index=False))

ranking.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\ranking_doc_level.csv", index=False)
df_univ.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\univ_doc_level.csv", index=False)
rf_imp.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\rf_imp_doc_level.csv", index=False)
if_imp.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\if_imp_doc_level.csv", index=False)

print("\nAll results saved.")
