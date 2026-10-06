import sys, json, time
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentence_transformers import SentenceTransformer

CORPUS = Path("data/processed/fewshot_retrieval/corpus.json")
GT_TEXTS = Path("data/processed/llm_benchmark/gt_texts.json")
OUT_DIR = Path("data/processed/fewshot_retrieval")
MODEL_NAME = "wilfredomartel/BGE-M3-Legal-Spanish"

def main():
    with open(CORPUS, encoding="utf-8") as f:
        corpus = json.load(f)
    with open(GT_TEXTS, encoding="utf-8") as f:
        gt_texts = json.load(f)

    model = SentenceTransformer(MODEL_NAME, device="cuda")

    t0 = time.time()
    all_out = {}
    for field, rows in corpus.items():
        texts = [r["text"] for r in rows]
        emb = model.encode(texts, normalize_embeddings=True, batch_size=64,
                           show_progress_bar=True)
        np.save(OUT_DIR / f"corpus_emb_{field}.npy", emb)
        all_out[field] = {"n": len(rows)}
        print(f"{field}: embedded {len(rows)} texts, {emb.shape}, "
              f"{(time.time()-t0):.1f}s cumulative")

    gt_emb = model.encode(list(gt_texts.values()), normalize_embeddings=True,
                          batch_size=64, show_progress_bar=True)
    np.save(OUT_DIR / "gt_emb.npy", gt_emb)
    np.save(OUT_DIR / "gt_keys.npy", np.array(list(gt_texts.keys())))
    print(f"gt texts embedded: {gt_emb.shape}, total {(time.time()-t0):.1f}s")

if __name__ == "__main__":
    main()
