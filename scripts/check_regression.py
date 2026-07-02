import pandas as pd
df = pd.read_parquet('./data/processed/test_2021/sections_2021_test.parquet')
print(f"Total sections: {len(df)}")
print(f"line_end -1: {(df['line_end']==-1).sum()}")
print(f"line_end >0: {(df['line_end']>0).sum()}")
print(f"line_end 0: {(df['line_end']==0).sum()}")
print(f"Any DATOS in content: {df['content'].str.contains('DATOS DE LA LICITACI[OÓ]N', na=False).sum()}")
