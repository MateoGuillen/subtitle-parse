"""Muestra los ítems de la muestra pseudo-GT de un campo para anotarlos (solo lectura, ciego).

Uso: /root/vllm-env/bin/python scripts/ver_muestra_pseudo_gt.py <campo> [desde] [hasta] [--max-chars N]
                                                                [--ventanas REGEX] [--ancho N]
Imprime id, descripción/guía (una vez) y el texto con espacios normalizados. Con --ventanas
muestra solo ±ancho caracteres alrededor de cada match (fusionando solapes): ayuda de lectura
para secciones largas; el valor de un plazo/porcentaje está necesariamente junto a su unidad.
"""
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
M = ROOT / "data/processed/llm_benchmark/pseudo_gt_opus/muestra.csv"


def ventanas(t, rx, w):
    spans = []
    for m in re.finditer(rx, t, re.I):
        a, b = max(0, m.start() - w), min(len(t), m.end() + w)
        if spans and a <= spans[-1][1]:
            spans[-1][1] = b
        else:
            spans.append([a, b])
    return " ... ".join(t[a:b] for a, b in spans) or "(sin coincidencias)"


def main():
    argv = sys.argv[1:]
    rx = w = None
    if "--ventanas" in argv:
        i = argv.index("--ventanas"); rx = argv[i + 1]; del argv[i:i + 2]
    w = 350
    if "--ancho" in argv:
        i = argv.index("--ancho"); w = int(argv[i + 1]); del argv[i:i + 2]
    sys.argv = [sys.argv[0]] + argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    mx = 6000
    if "--max-chars" in sys.argv:
        mx = int(sys.argv[sys.argv.index("--max-chars") + 1])
        args = [a for a in args if a != str(mx)]
    campo = args[0]
    lo = int(args[1]) if len(args) > 1 else 0
    hi = int(args[2]) if len(args) > 2 else 999
    d = pd.read_csv(M)
    d = d[d.campo == campo].reset_index(drop=True)
    print(f"### {campo} ({len(d)} ítems) — {d.descripcion.iloc[0]}\nGUÍA: {d.guia.iloc[0]}\n")
    for i, r in d.iloc[lo:hi].iterrows():
        t = re.sub(r"\s+", " ", str(r.texto)).strip()
        body = ventanas(t, rx, w) if rx else t[:mx]
        print(f"[{i}] id={r.id}\n{body}\n")


if __name__ == "__main__":
    main()
