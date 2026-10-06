"""Test write_to_dataset behavior."""
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import tempfile, shutil

schema = pa.schema([
    pa.field('document_id', pa.string()),
    pa.field('year', pa.string()),
    pa.field('title', pa.string()),
    pa.field('content_text', pa.string()),
])

df = pd.DataFrame({
    'document_id': ['d1', 'd2'],
    'year': ['2025', '2025'],
    'title': ['T1', 'T2'],
    'content_text': ['c1', 'c2'],
})

tmp = tempfile.mkdtemp()
try:
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=tmp,
        partition_cols=['year'],
        existing_data_behavior='overwrite_or_ignore',
        compression='snappy',
    )
    result = pd.read_parquet(tmp)
    year_col = result['year']
    print(f'year col dtype: {year_col.dtype}')
    print(f'year values: {year_col.tolist()}')
    all_match = (year_col == '2025').all()
    print(f'year == "2025": {all_match}')
finally:
    shutil.rmtree(tmp)
