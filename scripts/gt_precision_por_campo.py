"""GT precision report per field (no LLM re-run needed).

Reads the 26 per-field GT CSVs in data/processed/alolab/gt_por_campo/ and compares
llm_value (LLM champion output, already parsed) vs ground_truth (human).

Metrics per field:
  - n, GT with value vs NULL, LLM with value vs NULL
  - exact match  (gt == llm, and both-NULL counts as match)
  - flex match    (exact OR ratio x100 between gt and llm -> pct scale safe)
  - confusion matrix: TP / FN / FP / TN
  - precision, recall, accuracy (exact and flex)

For fields with exact accuracy < threshold (default 90%), exports the error rows
(gt, llm, llm_raw, nota, text) to gt_por_campo/errores_<campo>.csv for the
correction loop.

Usage (inside WSL, /root/vllm-env/bin/python is fine - only pandas):
  /root/vllm-env/bin/python scripts/gt_precision_por_campo.py
"""
import re, json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

FOLDER = Path("data/processed/alolab/gt_por_campo")
OUT_CSV = Path("data/processed/alolab/gt_precision_por_campo.csv")
OUT_MD = Path("data/processed/alolab/gt_precision_por_campo.md")
ERR_THRESHOLD = 90.0
ERR_TEXT_CHARS = 4000


def field_from_filename(name):
    b = re.sub(r"_GT \(1\)\.csv$|_gt \(1\)\.csv$", "", name)
    b = re.sub(r"_GT\.csv$|_gt\.csv$", "", b)
    return b


def to_num(series):
    """Empty/NaN -> None; else float (string numbers may use ',')."""
    out = []
    for v in series:
        if pd.isna(v):
            out.append(None)
            continue
        s = str(v).strip()
        if s == "":
            out.append(None)
            continue
        try:
            out.append(float(s.replace(",", ".")))
        except ValueError:
            out.append(None)
    return out


def match(gt, llm, flex=False):
    """Return True/False for a comparison. Both-None -> True."""
    if gt is None and llm is None:
        return True
    if gt is None or llm is None:
        return False
    if gt == llm:
        return True
    if flex and gt != 0 and llm != 0:
        if abs(gt / llm) == 100.0 or abs(llm / gt) == 100.0:
            return True
    return False


def main():
    files = sorted(f for f in FOLDER.glob("*.csv") if not f.name.startswith("errores_"))
    if not files:
        print(f"No CSVs in {FOLDER}")
        sys.exit(1)

    rows = []
    for f in files:
        campo = field_from_filename(f.name)
        df = pd.read_csv(f, encoding="utf-8")
        if "ground_truth" not in df.columns or "llm_value" not in df.columns:
            print(f"  [SKIP] {f.name}: missing ground_truth/llm_value")
            continue

        gt = to_num(df["ground_truth"])
        llm = to_num(df["llm_value"])
        n = len(df)

        exact = [match(g, l, flex=False) for g, l in zip(gt, llm)]
        flex = [match(g, l, flex=True) for g, l in zip(gt, llm)]

        gt_val = [g is not None for g in gt]
        llm_val = [l is not None for l in llm]

        tp = sum(1 for i in range(n) if gt_val[i] and llm_val[i] and flex[i])
        fn = sum(1 for i in range(n) if gt_val[i] and not llm_val[i])
        fp = sum(1 for i in range(n) if not gt_val[i] and llm_val[i])
        tn = sum(1 for i in range(n) if not gt_val[i] and not llm_val[i])

        tp_ex = sum(1 for i in range(n) if gt_val[i] and llm_val[i] and exact[i])
        fn_ex = fn
        fp_ex = fp
        tn_ex = tn

        acc_ex = (tp_ex + tn_ex) / n * 100
        acc_fl = (tp + tn) / n * 100
        prec_ex = tp_ex / (tp_ex + fp_ex) * 100 if (tp_ex + fp_ex) else float("nan")
        rec_ex = tp_ex / (tp_ex + fn_ex) * 100 if (tp_ex + fn_ex) else float("nan")

        rows.append({
            "campo": campo,
            "archivo": f.name,
            "n": n,
            "gt_valor": sum(gt_val),
            "gt_null": n - sum(gt_val),
            "llm_valor": sum(llm_val),
            "llm_null": n - sum(llm_val),
            "tp_flex": tp, "fn": fn, "fp": fp, "tn": tn,
            "acc_exacta": round(acc_ex, 1),
            "acc_flex": round(acc_fl, 1),
            "prec_exacta": round(prec_ex, 1) if not pd.isna(prec_ex) else None,
            "rec_exacta": round(rec_ex, 1) if not pd.isna(rec_ex) else None,
            "errores": n - tp_ex - tn_ex,
        })

        # export errors for weak fields
        if acc_ex < ERR_THRESHOLD:
            err_idx = [i for i in range(n) if not exact[i]]
            if err_idx:
                err_df = df.iloc[err_idx].copy()
                err_df["campo"] = campo
                if "text" in err_df.columns:
                    err_df["text"] = err_df["text"].apply(
                        lambda t: str(t)[:ERR_TEXT_CHARS])
                err_path = FOLDER / f"errores_{campo}.csv"
                err_df.to_csv(err_path, index=False, encoding="utf-8")
                print(f"  [export] {err_path.name}: {len(err_df)} errores")

    res = pd.DataFrame(rows).sort_values("acc_exacta", ascending=False)

    res.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # Markdown report
    lines = ["# GT Precision por campo (LLM vs humano)",
             "",
             f"- Archivos: {len(res)} campos",
             f"- Total juicios: {res['n'].sum()}",
             f"- GT con valor: {res['gt_valor'].sum()} | GT NULL: {res['gt_null'].sum()}",
             "- `acc_exacta`: gt==llm estricto. `acc_flex`: ademas acepta factor x100 (escala %).",
             "- TP/FN/FP/TN usan la comparacion flexible.",
             "",
             "| Campo | n | GT val | GT NULL | LLM val | acc% exacta | acc% flex | prec% | rec% | TP | FN | FP | TN |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
             ]
    for _, r in res.iterrows():
        lines.append(
            f"| {r['campo']} | {r['n']} | {r['gt_valor']} | {r['gt_null']} | "
            f"{r['llm_valor']} | {r['acc_exacta']} | {r['acc_flex']} | "
            f"{r['prec_exacta']} | {r['rec_exacta']} | {r['tp_flex']} | "
            f"{r['fn']} | {r['fp']} | {r['tn']} |")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    # console table
    print("\n=== GT PRECISION POR CAMPO ===")
    print(res.to_string(index=False))
    print(f"\nReport: {OUT_CSV}")
    print(f"Markdown: {OUT_MD}")


if __name__ == "__main__":
    main()
