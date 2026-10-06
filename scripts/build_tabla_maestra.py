"""Reconstruye tabla_maestra_benchmark.csv desde los JSON de resultados (plan F0.7, H5.4).

Columnas derivadas de multimodel_results/:
  gt100            <- {modelo}.json            (score/n, GT-100 humano)
  fase5_agree      <- fase5_v1/Fase5_*.json si existe, si no Fase5_*.json (pseudo-867 v1)
  fase5_v2_agree   <- Fase5_*.json con set pseudo900-v2 (solo top-3 re-corridos)
  single_*         <- latencias por ítem de la corrida Fase 5 v1 (P50/P95 nearest-rank)
  conc_*           <- Conc_*.json, nivel de concurrencia con mayor req/s ("techo")
Columnas estáticas (no están en los JSON): formato, gb, ctx, guided, flags -> META.

ADVERTENCIA (auditoría sesión 23, H2.1/H2.4): fase5_* y gt100 son acuerdo DESPUÉS
del postproceso determinista compartido; no miden la salida cruda del modelo.
Columnas sobre crudo (F6.4): c3_dias_crudo / c3_dias_null_baseline (F1, top-3) y
gt12_llm_only / gt12_pipeline (ablación contra GT de 12 campos, Qwen y FP8).

Uso:
  /root/vllm-env/bin/python scripts/build_tabla_maestra.py           # escribe CSV
  /root/vllm-env/bin/python scripts/build_tabla_maestra.py --check   # compara con CSV actual
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "data/processed/llm_benchmark/multimodel_results"
OUT = ROOT / "data/processed/llm_benchmark/tabla_maestra_benchmark.csv"

# modelo -> (archivo GT-100, Fase5, Conc) + metadatos estáticos
META = {
    "Llama 3.1 8B-FP8": dict(gt="Llama_31_8B-FP8", f5="Llama_31_8B-FP8", conc="Llama_FP8",
                             formato="FP8", gb=8.5, ctx="8192", guided="xgrammar (serve+offline)",
                             flags="quant auto FP8, bf16"),
    "Llama 3.1 8B-AWQ": dict(gt="Llama_31_8B-AWQ", f5="Llama_31_8B-AWQ", conc="Llama_AWQ",
                             formato="AWQ", gb=5.4, ctx="8192", guided="xgrammar (serve+offline)",
                             flags="HF_TOKEN tokenizer; quant auto"),
    "Qwen3-8B-FP8": dict(gt="Qwen3-8B-FP8", f5="Qwen3-8B-FP8", conc="Qwen3_8B_FP8",
                         formato="FP8", gb=8.9, ctx="4096", guided="xgrammar (serve+offline)",
                         flags="enable_thinking=False; gpu 0.80"),
    "Qwen3-14B-AWQ": dict(gt="Qwen3-14B-AWQ", f5="Qwen3-14B-AWQ", conc="Qwen3_14B",
                          formato="AWQ", gb=9.4, ctx="4096", guided="xgrammar (serve+offline)",
                          flags="enable_thinking=False; gpu 0.92"),
    "Ministral 3 14B-Q4_K_M": dict(gt="Ministral_3_14B", f5="Ministral_3_14B", conc="Ministral",
                                   formato="GGUF", gb=7.7, ctx="16384/4slots",
                                   guided="sin guided (plain+parse)", flags="llama-server np4"),
    "Mistral 7B v0.3-AWQ": dict(gt="Mistral_7B_v03-AWQ", f5="Mistral_7B_v03-AWQ", conc="Mistral",
                                formato="AWQ", gb=3.9, ctx="8192", guided="xgrammar (serve+offline)",
                                flags="merge_system obligatorio (jinja); quant auto"),
    "EXAONE-3.5-7.8B-AWQ": dict(gt="EXAONE-35-78B-AWQ", f5="EXAONE-35-78B-AWQ", conc="EXAONE",
                                formato="AWQ", gb=5.0, ctx="8192", guided="xgrammar (serve+offline)",
                                flags="-"),
    "Gemma 4 12B-Q4_K_XL": dict(gt="Gemma_4_12B", f5="Gemma_4_12B", conc="Gemma4",
                                formato="GGUF", gb=6.3, ctx="16384/4slots",
                                guided="sin guided (plain+parse)",
                                flags="--reasoning off servidor; np4"),
    "Granite 4.2 8B-Q4_K_M": dict(gt="Granite_42_8B", f5="Granite_42_8B", conc="Granite",
                                  formato="GGUF", gb=5.2, ctx="16384/4slots",
                                  guided="sin guided (plain+parse)",
                                  flags="enable_thinking:false; llama-server np4"),
    "Mistral Small 24B-AWQ": dict(gt="Mistral_Small_24B-AWQ", f5="Mistral_Small_24B-AWQ",
                                  conc="Mistral_Small", formato="AWQ", gb=14.0, ctx="2048",
                                  guided="xgrammar offline (serve cuelga)",
                                  flags="cpu_offload 5GB; maxseqs 2-4; trunc >2048"),
}
COLS = ["modelo", "formato", "gb", "ctx", "guided", "gt100", "flags", "fase5_agree",
        "single_P50_s", "single_P95_s", "single_elapsed_s", "conc_N", "conc_backend",
        "conc_techo_req_s", "conc_C_techo", "conc_P50_C1_s", "conc_P95_techo_s",
        "conc_VRAM_MiB", "conc_agree", "fase5_v2_agree",
        "c3_dias_crudo", "c3_dias_null_baseline", "gt12_llm_only", "gt12_pipeline"]
# Sesión 23 (F6.4): columnas sobre SALIDA CRUDA (F1) y ablación contra GT (F4.5).
F1_CSV = ROOT / "data/processed/llm_benchmark/F1_rescore_crudo.csv"
ABL_CSV = ROOT / "data/processed/llm_benchmark/F6_ablacion_resumen.csv"
F1_TAG = {"Llama 3.1 8B-FP8": "FP8", "Llama 3.1 8B-AWQ": "AWQ", "Qwen3-8B-FP8": "QWEN"}
ABL_TAG = {"Llama 3.1 8B-FP8": "Llama-3.1-8B-FP8", "Qwen3-8B-FP8": "Qwen3-8B-FP8"}


def load(name: str) -> dict:
    return json.loads((RES / name).read_text(encoding="utf-8"))


def pct(vals, q):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(round(q * (len(vals) - 1))))]


def frac(k, n):
    return f"{k}/{n}={100 * k / n:.1f}%"


def _crudo_cols(modelo: str) -> dict:
    out = {"c3_dias_crudo": "", "c3_dias_null_baseline": "", "gt12_llm_only": "",
           "gt12_pipeline": ""}
    if modelo in F1_TAG and F1_CSV.exists():
        f1 = pd.read_csv(F1_CSV)
        sel = f1[(f1.experimento == "C3_288_humano") & (f1.modelo == F1_TAG[modelo])
                 & (f1.campo == "DIAS_239")].set_index("metrica")
        out["c3_dias_crudo"] = frac(int(sel.loc["crudo", "k"]), int(sel.loc["crudo", "n"]))
        out["c3_dias_null_baseline"] = frac(int(sel.loc["null_bl", "k"]), int(sel.loc["null_bl", "n"]))
    if modelo in ABL_TAG and ABL_CSV.exists():
        ab = pd.read_csv(ABL_CSV)
        d = ab[ab.modelo == ABL_TAG[modelo]]
        out["gt12_llm_only"] = frac(int(d["llm"].sum()), int(d["n"].sum()))
        out["gt12_pipeline"] = frac(int(d["pipeline"].sum()), int(d["n"].sum()))
    return out


def build() -> pd.DataFrame:
    rows = []
    for modelo, m in META.items():
        gt = load(f"{m['gt']}.json")
        v1_path = RES / "fase5_v1" / f"Fase5_{m['f5']}.json"
        main = load(f"Fase5_{m['f5']}.json")
        is_v2 = str(main.get("set", "")).startswith("pseudo900-v2")
        v1 = json.loads(v1_path.read_text(encoding="utf-8")) if v1_path.exists() else main
        if is_v2 and not v1_path.exists():
            raise SystemExit(f"{modelo}: Fase5 v2 sin backup v1 en fase5_v1/")
        r1 = v1["results"]
        lat = [x["lat"] for x in r1 if x.get("lat") is not None]
        conc = load(f"Conc_{m['conc']}.json")
        L = conc["levels"]
        top = max(L, key=lambda k: L[k]["req_per_s"])
        rows.append({
            "modelo": modelo, "formato": m["formato"], "gb": m["gb"], "ctx": m["ctx"],
            "guided": m["guided"], "gt100": f"{gt['score']}/{len(gt['results'])}",
            "flags": m["flags"],
            "fase5_agree": frac(sum(bool(x["agree"]) for x in r1), len(r1)),
            "single_P50_s": round(pct(lat, .5), 2), "single_P95_s": round(pct(lat, .95), 2),
            "single_elapsed_s": round(v1["elapsed"]),
            "conc_N": conc["prompts"], "conc_backend": conc["backend"],
            "conc_techo_req_s": L[top]["req_per_s"], "conc_C_techo": top,
            "conc_P50_C1_s": L.get("1", {}).get("p50_s"),
            "conc_P95_techo_s": L[top].get("p95_s"), "conc_VRAM_MiB": L[top]["vram_peak_mib"],
            "conc_agree": f"{L[top]['agree']}/{L[top]['n']}",
            "fase5_v2_agree": (frac(sum(bool(x["agree"]) for x in main["results"]),
                                    len(main["results"])) if is_v2 else ""),
            **_crudo_cols(modelo),
        })
    return pd.DataFrame(rows, columns=COLS)


def check(new: pd.DataFrame) -> int:
    old = pd.read_csv(OUT, dtype=str).fillna("")
    new = new.astype(object).where(new.notna(), "").astype(str)
    bad = 0
    for col in [c for c in old.columns if c in new.columns]:
        for i, (a, b) in enumerate(zip(old[col], new[col])):
            try:
                same = abs(float(a) - float(b)) < 1e-9
            except ValueError:
                same = a == b
            if not same:
                bad += 1
                print(f"DIFF {new.loc[i, 'modelo']:24s} {col}: csv={a!r} build={b!r}")
    print(f"{'OK' if not bad else 'FALLA'}: {len(old)} filas, {bad} celdas distintas "
          f"(columnas nuevas: {[c for c in new.columns if c not in old.columns]})")
    return 0 if not bad else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    df = build()
    if a.check:
        sys.exit(check(df))
    df.to_csv(OUT, index=False)
    print(f"{len(df)} filas -> {OUT.relative_to(ROOT)}")
