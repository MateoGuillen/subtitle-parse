import pandas as pd
import numpy as np

# Load saved results
ranking = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\final_ranking.csv")
univ = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\univariate_results.csv")
rf_imp = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\rf_importances.csv")
if_imp = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\if_importances.csv")
vif = pd.read_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\vif_results.csv")

# Apply multicollinearity reduction decisions
# 1. content_length_clean (identical to content_length, r=1.0) -> DROP
# 2. line_span (VIF=999, perfectly predicted by line_end - line_start) -> DROP
# 3. estimated_tokens (highly correlated with size_bytes r=0.96, lower domain score) -> DROP
# 4. tokens_per_line (lowest in every category) -> DROP

features_to_drop = ['content_length_clean', 'line_span', 'estimated_tokens', 'tokens_per_line']
ranking_filtered = ranking[~ranking['feature'].isin(features_to_drop)].copy()
ranking_filtered.sort_values('final_score', ascending=False, inplace=True)
ranking_filtered['rank'] = range(1, len(ranking_filtered) + 1)

print("=== FINAL TOP 10 FEATURES (after redundancy removal) ===")
cols = ['rank', 'feature', 'final_score', 'outlier_score', 'domain_score', 
        'rf_importance_norm', 'if_importance_norm']
print(ranking_filtered[cols].round(4).to_string(index=False))

# Save final clean ranking
ranking_filtered.to_csv("C:\\Users\\Giovanni\\AppData\\Local\\Temp\\opencode\\final_ranking_clean.csv", index=False)
print("\nSaved clean ranking to final_ranking_clean.csv")
