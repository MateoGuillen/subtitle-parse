import sys; sys.path.insert(0, r'D:\projects\subtitle-parse')
from src.pipelines.title_ranking_pipeline import TitleRankingPipeline
from config.settings import DB_CONFIG
p = TitleRankingPipeline({'db_params': DB_CONFIG, 'top_n': 80, 'section_sample': 50000, 'output_dir': r'D:\projects\subtitle-parse\data\processed\title_ranking'})
r = p.run()
print('Resultado:', r)
