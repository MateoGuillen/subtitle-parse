"""I4 — Aplica al GT las correcciones aprobadas por el usuario (planilla i4_realineacion.csv).

No modifica los originales (siguen reproduciendo los resultados publicados). Escribe copias alineadas con la
especificación v1.0:
  data/processed/alolab/gt_por_campo_v1/<campo>_GT.csv    ground_truth corregido + ground_truth_agosto (original)
                                                          + realineacion_i4 (decisión de ficha que lo explica)
  data/processed/llm_benchmark/pseudo_gt_opus/anotaciones_v1.json   mismo formato que anotaciones.json
  data/processed/llm_benchmark/planillas/i4_aplicado.csv  bitácora: id, original, final, decisión

Aprobación: 08-oct-2026, "apruebo todo" (85 filas; la fila 'revisar' 463117 endeudamiento -> 0,8 según el
pliego "No deberá ser mayor a 0,80"). Las filas con aprobar='n' se saltean; valor_final pisa la propuesta.
Uso: /root/vllm-env/bin/python scripts/aplicar_i4_gt.py [--todo-aprobado]
"""
import argparse
import json
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
GT_DIR = ROOT / "data/processed/alolab/gt_por_campo"
GT_V1 = ROOT / "data/processed/alolab/gt_por_campo_v1"
PS = ROOT / "data/processed/llm_benchmark/pseudo_gt_opus"
PLAN = ROOT / "data/processed/llm_benchmark/planillas/i4_realineacion.csv"
LOG = ROOT / "data/processed/llm_benchmark/planillas/i4_aplicado.csv"
# decisión del usuario sobre la fila sin ficha (leída en el pliego, ver docstring)
VALOR_REVISAR = {"ratio_endeudamiento_maximo|463117": 0.8}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--todo-aprobado", action="store_true", help="aprobación global ('apruebo todo')")
    a = ap.parse_args()
    plan = pd.read_csv(PLAN, dtype={"id": str, "nro_licitacion": str})
    if a.todo_aprobado:
        plan["aprobar"] = plan["aprobar"].fillna("s")
    for k, v in VALOR_REVISAR.items():
        plan.loc[(plan.id == k) & plan.valor_final.isna(), "valor_final"] = v
    plan["aprobar"] = plan["aprobar"].astype(str).str.strip().str.lower()
    ok = plan[plan.aprobar.isin(["s", "si", "sí", "1", "true"])].copy()
    pend = plan[(plan.decision == "revisar") & plan.valor_final.isna()]
    if len(ok) != len(plan) and not len(ok):
        raise SystemExit("ninguna fila aprobada")
    if len(pend):
        raise SystemExit(f"filas 'revisar' sin valor_final: {pend.id.tolist()}")
    ok["final"] = ok.valor_final.where(ok.valor_final.notna(), ok.valor_propuesto)
    ok.loc[ok.decision == "revisar", "final"] = ok.valor_final
    log = []

    # ---- GT de agosto: copia completa del directorio + correcciones
    GT_V1.mkdir(parents=True, exist_ok=True)
    fixes = {r.id: r for r in ok[ok.referencia == "gt_agosto"].itertuples()}
    for p in sorted(GT_DIR.glob("*.csv")):
        if p.name.startswith("errores_"):
            continue
        g = pd.read_csv(p, dtype={"nro_licitacion": str})
        if g.empty or "ground_truth" not in g:
            shutil.copy2(p, GT_V1 / p.name)
            continue
        f = g.campo.iloc[0]
        g["ground_truth_agosto"] = g.ground_truth
        g["realineacion_i4"] = ""
        for i, r in g.iterrows():
            fx = fixes.get(f"{f}|{r.nro_licitacion}")
            if fx is None:
                continue
            g.at[i, "ground_truth"] = fx.final
            g.at[i, "realineacion_i4"] = fx.decision
            log.append({"referencia": "gt_agosto", "id": fx.id, "campo": f, "original": r.ground_truth,
                        "final": fx.final, "decision": fx.decision})
        g.to_csv(GT_V1 / p.name, index=False)

    # ---- pseudo-GT Opus
    ann = json.loads((PS / "anotaciones.json").read_text(encoding="utf-8"))
    for r in ok[ok.referencia == "pseudo_opus"].itertuples():
        old = ann[r.id][0]
        ann[r.id] = [None if pd.isna(r.final) else float(r.final), ann[r.id][1],
                     (ann[r.id][2] + " | " if ann[r.id][2] else "") + f"I4 {r.decision}: {old} -> {r.final}"]
        log.append({"referencia": "pseudo_opus", "id": r.id, "campo": r.campo, "original": old, "final": r.final,
                    "decision": r.decision})
    (PS / "anotaciones_v1.json").write_text(json.dumps(ann, ensure_ascii=False, indent=1), encoding="utf-8")

    pd.DataFrame(log).to_csv(LOG, index=False)
    print(f"aprobadas {len(ok)}/{len(plan)} · aplicadas {len(log)} "
          f"(gt_agosto {sum(x['referencia'] == 'gt_agosto' for x in log)}, "
          f"pseudo_opus {sum(x['referencia'] == 'pseudo_opus' for x in log)})")
    if len(log) != len(ok):
        raise SystemExit("ERROR: no todas las filas aprobadas encontraron su ítem en el GT")
    print(f"-> {GT_V1.relative_to(ROOT)}/ · {(PS / 'anotaciones_v1.json').relative_to(ROOT)} · "
          f"{LOG.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
