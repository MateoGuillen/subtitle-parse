# -*- coding: utf-8 -*-
"""
Genera el set completo de graficas para la presentacion de tutores.
Estilo consistente (paleta de la tesis), iconos Font Awesome y datos
verificados contra el repo.

Ejecutar:  python docs/presentacion_tutores/generar_graficas.py
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle
import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "graficos")
FONT_PATH = os.path.join(BASE, "fonts", "fa-solid-900.ttf")
os.makedirs(OUT, exist_ok=True)
fm.fontManager.addfont(FONT_PATH)
FA = fm.FontProperties(fname=FONT_PATH)

# ---- Paleta de la tesis ----
DARK = "#222A35"
BLUE = "#2C6FBB"
GREEN = "#2E8B57"
ORANGE = "#C77F2C"
PURPLE = "#7A3FA0"
RED = "#C0392B"
GRAY = "#5A6472"
LIGHT = "#EEF3FA"
BGRID = "#D8DEE7"

plt.rcParams.update({
    "font.size": 11.5,
    "axes.titleweight": "bold",
    "axes.titlesize": 15,
    "axes.labelsize": 12.5,
    "axes.edgecolor": BGRID,
    "axes.grid": True,
    "grid.color": BGRID,
    "grid.linewidth": 0.7,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "font.family": "DejaVu Sans",
})

CHECK_OVERLAPS = False


def save(fig, name, tight=True):
    if tight:
        fig.tight_layout()
    if CHECK_OVERLAPS:
        from matplotlib.text import Annotation
        fig.canvas.draw()
        rend = fig.canvas.get_renderer()
        texts = []
        for ax_ in fig.axes:
            for t in ax_.texts:
                if isinstance(t, Annotation) and t.arrow_patch is not None:
                    continue  # bbox de anotacion con flecha incluye la flecha (falso positivo)
                bb = t.get_window_extent(renderer=rend)
                if bb.width > 0 and bb.height > 0:
                    texts.append((t.get_text()[:28], bb))
        problems = []
        for i in range(len(texts)):
            for j in range(i + 1, len(texts)):
                if texts[i][1].overlaps(texts[j][1]):
                    problems.append((texts[i][0], texts[j][0]))
        if problems:
            print(f"  OVERLAP [{name}] {len(problems)}:")
            for a, b in problems[:12]:
                print(f"     '{a}'  <->  '{b}'")
    fig.savefig(os.path.join(OUT, name), dpi=170, bbox_inches="tight")
    plt.close(fig)
    print("OK", name)


def fuente(ax, txt):
    ax.text(0.0, -0.13, txt, transform=ax.transAxes, fontsize=9.0, color=GRAY)


def icon(ax, x, y, code, size=22, color=DARK, ha="center", va="center", zorder=6, **kw):
    ax.text(x, y, code, fontproperties=FA, fontsize=size, color=color,
            ha=ha, va=va, zorder=zorder, **kw)


# =========================================================================
# 1. flujo_pipeline.png  —  ARQUITECTURA ETL (Extracción-Transformación-Carga)
# =========================================================================
def badge(ax, cx, cy, r, code, bg, fg):
    c = Circle((cx, cy), r, facecolor=bg, edgecolor="white", linewidth=2, zorder=5)
    ax.add_patch(c)
    icon(ax, cx, cy, code, size=r * 2.2, color=fg)


def flow_box(ax, x, y, w, h, icon_code, title, body, color, title_size=10.5, body_size=8.0):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.5,rounding_size=1.2",
                       linewidth=1.8, edgecolor=color, facecolor="white", zorder=2)
    ax.add_patch(p)
    badge(ax, x + 6.5, y + h - 4.6, 2.8, icon_code, color, "white")
    ax.text(x + 11.0, y + h - 4.6, title, fontsize=title_size, fontweight="bold",
            color=DARK, ha="left", va="center")
    ax.plot([x + 2.5, x + w - 2.5], [y + h - 8.2, y + h - 8.2],
            color=BGRID, lw=1.0, zorder=2)
    ax.text(x + 3.0, y + h - 9.3, body, fontsize=body_size, color=GRAY, ha="left",
            va="top", linespacing=1.4, zorder=2)


def mini_box(ax, x, y, w, h, icon_code, title, body, color, title_size=8.3):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.0",
                       linewidth=1.5, edgecolor=color, facecolor="white", zorder=2)
    ax.add_patch(p)
    icon(ax, x + 3.2, y + h - 3.6, icon_code, size=10.5, color=color)
    ax.text(x + 7.0, y + h - 3.6, title, fontsize=title_size, fontweight="bold",
            color=DARK, ha="left", va="center")
    ax.text(x + 2.2, y + h - 6.6, body, fontsize=6.9, color=GRAY, ha="left",
            va="top", linespacing=1.3)


def stage_container(ax, x, y, w, h, color, tag, tag_color=None, letter=None):
    """Caja grande de etapa con borde de color y etiqueta arriba-izquierda."""
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.5,rounding_size=1.4",
                       linewidth=2.2, edgecolor=color, facecolor="#FFFFFF", zorder=1)
    ax.add_patch(p)
    if letter:
        c = Circle((x + 4.4, y + h - 4.0), 3.0, facecolor=color, edgecolor="white",
                   linewidth=1.5, zorder=6)
        ax.add_patch(c)
        icon(ax, x + 4.4, y + h - 4.0, letter, size=13, color="white", zorder=7)
        ax.text(x + 9.2, y + h - 4.0, tag, fontsize=10.5, fontweight="bold",
                color=tag_color or color, ha="left", va="center")


def varrow(ax, x1, y1, x2, y2, color=GRAY, lw=2.2, label=None, lx=None, ly=None):
    a = FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                        mutation_scale=15, linewidth=lw, color=color, zorder=3)
    ax.add_patch(a)
    if label:
        ax.text(lx or ((x1 + x2) / 2), ly or ((y1 + y2) / 2), label,
                fontsize=7.6, color=GRAY, ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                          edgecolor=BGRID, lw=0.8), zorder=4)


def chart_flujo():
    fig, ax = plt.subplots(figsize=(11.6, 12.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 172)
    ax.axis("off")

    # Titulo
    ax.text(50, 168.5, "Pipeline ETL: de las fuentes públicas a la detección de anomalías",
            ha="center", fontsize=17, fontweight="bold", color=DARK)
    ax.plot([14, 86], [166, 166], color=BLUE, lw=2.4)

    # ---------- FUENTES ----------
    ax.text(50, 162.5, "FUENTE DE DATOS  ·  Portal DNCP  contrataciones.gov.py",
            ha="center", fontsize=10, fontweight="bold", color=BLUE)
    flow_box(ax, 8, 146.5, 40, 14, "\uf1c6", "Bulk OCDS  masivo.zip",
             "CSV estructurados por año\n(73-462 MB cada uno)", BLUE, title_size=10, body_size=7.6)
    flow_box(ax, 52, 146.5, 40, 14, "\uf1c1", "PDFs de pliegos",
             "Texto NO estructurado\n(pliego de bases y condiciones)", ORANGE, title_size=10, body_size=7.6)
    varrow(ax, 28, 146, 28, 140.8, BLUE)
    varrow(ax, 72, 146, 72, 140.8, ORANGE)

    # ---------- E · EXTRACCION ----------
    stage_container(ax, 6, 97, 88, 43, GREEN, "E · EXTRACCIÓN — tratamiento completo", letter="E")
    # fila A: 3 pasos
    mini_box(ax, 10, 123.5, 25, 10.5, "\uf1c6", "1 · Descomprimir",
             "masivo.zip → 18 CSVs\n(records, awards, contracts…)", GREEN)
    mini_box(ax, 37.5, 123.5, 25, 10.5, "\uf0b0", "2 · Filtrar pliegos",
             "solo pliego electrónico\nwhitelist 31,387", GREEN)
    mini_box(ax, 65, 123.5, 25, 10.5, "\uf019", "3 · Descargar PDFs",
             "asíncrona (batch 1,000)\n32,785 PDFs", GREEN)
    varrow(ax, 22.5, 123.2, 22.5, 120.3, GREEN, lw=1.4)
    varrow(ax, 50, 123.2, 50, 120.3, GREEN, lw=1.4)
    varrow(ax, 77.5, 123.2, 77.5, 120.3, GREEN, lw=1.4)
    # fila B: 3 pasos
    mini_box(ax, 10, 109.5, 25, 10.5, "\uf15c", "4 · Texto + outlines",
             "Apache Tika (línea + página)\n+ PyPDF2 outlines (prof. 1-2)", GREEN)
    mini_box(ax, 37.5, 109.5, 25, 10.5, "\uf5fd", "5 · Cruce → secciones",
             "outlines × líneas\n5 casos de límite → 2.78M", GREEN)
    mini_box(ax, 65, 109.5, 25, 10.5, "\uf12d", "6 · Limpiar",
             "guiones·páginas·Unicode\nstemming → secciones limpias", GREEN)
    varrow(ax, 22.5, 109.2, 22.5, 107.8, GREEN, lw=1.4)
    varrow(ax, 50, 109.2, 50, 107.8, GREEN, lw=1.4)
    varrow(ax, 77.5, 109.2, 77.5, 107.8, GREEN, lw=1.4)
    # resultado extraccion
    mini_box(ax, 26, 98.5, 48, 9, "\uf5fd", "Secciones + texto limpio",
             "pliegos_secciones · ~2.78M filas · Parquet", GREEN)

    varrow(ax, 50, 96.5, 50, 92.5, GREEN)

    # ---------- T · TRANSFORMACION ----------
    stage_container(ax, 6, 60, 88, 32, PURPLE, "T · TRANSFORMACIÓN — conversión regex / LLM", letter="T")
    mini_box(ax, 10, 76, 38, 10.5, "\uf013", "Regex determinista",
             "69 campos · patrón plantilla\n≥90% GT · sin GPU", PURPLE)
    mini_box(ax, 52, 76, 38, 10.5, "\uf5dc", "LLM hybrid (fallback)",
             "12 campos · Llama 3.1 8B AWQ\n+ postproceso determinista", PURPLE)
    varrow(ax, 29, 75.7, 29, 72.3, PURPLE, lw=1.4)
    varrow(ax, 71, 75.7, 71, 72.3, PURPLE, lw=1.4)
    # features
    mini_box(ax, 9, 62.5, 26, 9, "\uf0db", "document_features_v2",
             "81 campos (69+12)", PURPLE, title_size=7.5)
    mini_box(ax, 37, 62.5, 26, 9, "\uf201", "document_economic_features",
             "46 cols económicas", PURPLE, title_size=7.5)
    mini_box(ax, 65, 62.5, 26, 9, "\uf0db", "document_features",
             "571 cols estructurales", PURPLE, title_size=7.5)

    varrow(ax, 50, 59.5, 50, 56.5, PURPLE)

    # ---------- L · CARGA ----------
    stage_container(ax, 6, 31, 88, 25, BLUE, "L · CARGA  —  PostgreSQL", letter="L")
    box = FancyBboxPatch((10, 34), 80, 15, boxstyle="round,pad=0.5,rounding_size=1.2",
                         linewidth=1.6, edgecolor=BLUE, facecolor="#EAF2FC", zorder=2)
    ax.add_patch(box)
    badge(ax, 18, 44, 2.8, "\uf1c0", BLUE, "white")
    ax.text(23, 44.5, "Base de datos  dncp  (PostgreSQL)", fontsize=11,
            fontweight="bold", color=BLUE, va="center")
    ax.text(23, 39.2, "17 tablas OCDS  (convocantes → protestas)\npliegos_secciones  (2.78M filas · COPY batches 10k)\nfeatures: v2 (81) · económica (46) · estructural (571)",
            fontsize=8.2, color=DARK, va="center", linespacing=1.5)

    varrow(ax, 50, 30.5, 50, 27.2, BLUE)

    # ---------- ALGORITMO ----------
    box = FancyBboxPatch((10, 11.5), 80, 15, boxstyle="round,pad=0.5,rounding_size=1.2",
                         linewidth=2.4, edgecolor=RED, facecolor="#FDECEA", zorder=2)
    ax.add_patch(box)
    badge(ax, 17, 19, 3.2, "\uf024", RED, "white")
    icon(ax, 27, 19, "\uf002", size=19, color=RED)
    ax.text(33, 21.8, "APLICACIÓN DEL ALGORITMO  ·  detección de anomalías", fontsize=11.5,
            fontweight="bold", color=RED, va="center")
    ax.text(33, 16.8, "Red flags por licitación · ranking ponderado (6 estrategias)\nvalidación externa con protestas (1,253)  →  estudio de anomalías",
            fontsize=8.6, color=DARK, va="center", linespacing=1.45)

    ax.text(50, 7, "ETL: Extracción → Transformación → Carga → Algoritmo · datos 2021-2025/2026",
            ha="center", fontsize=9.5, color=GRAY)
    save(fig, "flujo_pipeline.png")


# =========================================================================
# 2. tamano_masivo_zip.png
# =========================================================================
def chart_zips():
    years = [2021, 2022, 2023, 2024, 2025, 2026]
    mb = [73.0, 258.7, 164.6, 319.2, 462.0, 5.9]
    colors = [BLUE] * 5 + [GRAY]
    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    bars = ax.bar([str(y) for y in years], mb, color=colors, width=0.6,
                  edgecolor="white")
    for b, v in zip(bars, mb):
        ax.text(b.get_x() + b.get_width() / 2, v + 12, f"{v:.0f} MB",
                ha="center", fontsize=10, fontweight="bold", color=DARK)
    ax.set_title("Bulk OCDS  masivo_{year}.zip  —  datos estructurados (no API)")
    ax.set_xlabel("Año")
    ax.set_ylabel("Tamaño del zip (MB)")
    ax.set_ylim(0, 540)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.text(0.98, 0.94, "Total ~1,283 MB  (6 zips)", transform=ax.transAxes,
            ha="right", fontsize=10, color=GRAY, style="italic",
            bbox=dict(boxstyle="round,pad=0.4", facecolor=LIGHT, edgecolor=BGRID))
    icon(ax, 0.035, 0.94, "\uf1c6", size=18, color=GRAY, transform=ax.transAxes,
         ha="center", va="center")
    fuente(ax, "Fuente: data/raw/ — masivo_{year}.zip verificados en disco")
    save(fig, "tamano_masivo_zip.png")


# =========================================================================
# 3. pdfs_por_anio.png
# =========================================================================
def chart_pdfs():
    years = [2021, 2022, 2023, 2024, 2025, 2026]
    pdfs = [1116, 4620, 6237, 9406, 10805, 601]
    colors = [BLUE] * 5 + [GRAY]
    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    bars = ax.bar([str(y) for y in years], pdfs, color=colors, width=0.6,
                  edgecolor="white")
    for b, v in zip(bars, pdfs):
        ax.text(b.get_x() + b.get_width() / 2, v + 140, f"{v:,}",
                ha="center", fontsize=10, fontweight="bold", color=DARK)
    ax.set_title("PDFs de pliegos descargados por año  (total 32,785)")
    ax.set_xlabel("Año")
    ax.set_ylabel("PDFs descargados")
    ax.set_ylim(0, 12600)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.text(0.98, 0.94, "Crecimiento ×10  (2021 → 2025)", transform=ax.transAxes,
            ha="right", fontsize=10, color=RED, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.35", facecolor="#FDECEA", edgecolor=RED, lw=0.8))
    icon(ax, 0.035, 0.94, "\uf1c1", size=18, color=GRAY, transform=ax.transAxes,
         ha="center", va="center")
    fuente(ax, "Fuente: data/raw/pdf/{year}/download_summary_pdf_{year}.csv (conteo real)")
    save(fig, "pdfs_por_anio.png")


# =========================================================================
# 4. volumenes_capas.png  (funnel en escala log)
# =========================================================================
def chart_volumenes():
    capas = ["Whitelist\n(pliegos electron.)", "PDFs\ndescargados",
             "Secciones\n(pliegos_secciones)", "Docs con\nfeatures v2"]
    vol = [31387, 32785, 2775485, 31321]
    colors = [BLUE, GREEN, ORANGE, PURPLE]
    fig, ax = plt.subplots(figsize=(10.5, 5.4))
    bars = ax.bar(capas, vol, color=colors, width=0.55, edgecolor="white")
    ax.set_yscale("log")
    for b, v in zip(bars, vol):
        ax.text(b.get_x() + b.get_width() / 2, v * 1.35, f"{v:,}",
                ha="center", fontsize=11, fontweight="bold", color=DARK)
    ax.set_title("Volumen por capa del pipeline  (escala log)  —  del PDF a la feature")
    ax.set_ylabel("Filas / documentos (log)")
    ax.set_ylim(1e2, 1e7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.add_patch(FancyArrowPatch((0.15, 2e5), (3.4, 31321), arrowstyle="-|>",
                                 mutation_scale=14, linewidth=1.4, color=RED))
    ax.text(0.05, 4.2e5, "agrega por\nlicitación", fontsize=8.6, color=RED,
            style="italic", ha="left", va="bottom")
    icon(ax, 0.035, 0.94, "\uf5fd", size=18, color=GRAY, transform=ax.transAxes,
         ha="center", va="center")
    fuente(ax, "Fuente: whitelist CSV, disco PDFs, dncp.pliegos_secciones, AGENTS.md")
    save(fig, "volumenes_capas.png")


# =========================================================================
# 5. campos_schema.png  (donut: 69 regex + 9 hybrid + 3 irresolubles)
# =========================================================================
def chart_campos():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.8, 5.4),
                                   gridspec_kw={"width_ratios": [1, 1.3]})
    sizes = [69, 9, 3]
    labels = ["regex\n69", "hybrid (LLM)\n9", "irresolubles\n3"]
    colors = [BLUE, PURPLE, RED]
    codes = ["\uf013", "\uf5dc", "\uf071"]
    wedges, _ = ax1.pie(sizes, colors=colors, startangle=90,
                        wedgeprops=dict(width=0.42, edgecolor="white"))
    ax1.text(0, 0.10, "81", ha="center", va="center", fontsize=30,
             fontweight="bold", color=DARK)
    ax1.text(0, -0.20, "campos", ha="center", va="center", fontsize=12, color=GRAY)
    ax1.set_xlim(-1.55, 1.55)
    ax1.set_ylim(-1.35, 1.35)
    ax1.legend(wedges, labels, loc="center left", bbox_to_anchor=(0.94, 0.5),
               frameon=False, fontsize=10)
    for w, c in zip(wedges, codes):
        ang = np.deg2rad((w.theta1 + w.theta2) / 2)
        icon(ax1, 1.06 * np.cos(ang), 1.06 * np.sin(ang), c, size=12, color="white")

    ax2.axis("off")
    ax2.set_title("Cómo se extrae cada campo", loc="left", pad=12)
    rows = [
        ("Regex (69)  determinista, sin GPU", BLUE,
         ["pct_* de plantilla (5)", "binarios de presencia (keyword)",
          "días de plantilla", "conteo de ítems nivel 1"]),
        ("Hybrid (9)  LLM + postproceso", PURPLE,
         ["días con cláusula asociada", "ratios decimales en prosa",
          "desambiguación solicitar/abonar", "conversión meses → días"]),
        ("Irresolubles (3)  se mantienen hybrid", RED,
         ["num_componentes_formula", "num_documentos_pago",
          "num_pasos_verificacion"]),
    ]
    y = 0.95
    for title, color, items in rows:
        ax2.text(0.02, y, title, fontsize=11, fontweight="bold", color=color,
                 transform=ax2.transAxes)
        y -= 0.058
        for it in items:
            ax2.text(0.05, y, "• " + it, fontsize=9.6, color=DARK,
                     transform=ax2.transAxes)
            y -= 0.047
        y -= 0.018
    save(fig, "campos_schema.png")


# =========================================================================
# 6. comparativa_estrategias.png
# =========================================================================
def chart_estrategias():
    csvp = os.path.join(BASE, "..", "..", "data", "processed", "alolab",
                        "comparativa_estrategias.csv")
    df = pd.read_csv(csvp)
    cols = {"acc_flex_r": "R\nregex", "acc_flex_ri": "Ri\nregex+post",
            "acc_flex_l_raw": "L_raw\nLLM bruto", "acc_flex_l_pp": "L_pp\nLLM+post",
            "acc_flex_h": "H\nhíbrido"}
    present = [c for c in cols if c in df.columns]
    means = df[present].mean().round(1)
    colors = [BLUE, BLUE, RED, PURPLE, GREEN]
    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    bars = ax.bar([cols[c] for c in present], means.values, color=colors,
                  width=0.62, edgecolor="white")
    for b, v in zip(bars, means.values):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.8, f"{v:.1f}",
                ha="center", fontsize=11, fontweight="bold", color=DARK)
    ax.set_title("Precisión media por estrategia de extracción  (26 campos, 5,066 juicios GT)")
    ax.set_ylabel("Acierto medio (%)")
    ax.set_ylim(0, 90)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.text(3, 72, "+12 pp\n(postproceso)", fontsize=8.0, color=PURPLE,
            ha="center", fontweight="bold")
    ax.text(4, 74.5, "+5 pp\n(híbrido)", fontsize=8.0, color=GREEN,
            ha="center", fontweight="bold")
    ax.plot([2.6, 3.0], [71, 66.0], color=PURPLE, lw=1.2, alpha=0.7)
    ax.plot([3.75, 4.0], [74.2, 71.0], color=GREEN, lw=1.2, alpha=0.7)
    fuente(ax, "Fuente: data/processed/alolab/comparativa_estrategias.csv")
    save(fig, "comparativa_estrategias.png")


# =========================================================================
# 7. portes_regex.png  (barras con etiquetas verticales)
# =========================================================================
def chart_portes():
    csvp = os.path.join(BASE, "..", "..", "data", "processed", "alolab",
                        "comparativa_estrategias.csv")
    df = pd.read_csv(csvp).set_index("campo")
    portados = ["num_categorias_documentos", "num_referencias_minimo",
                "num_referencias_tecnicas_minimo", "pct_anticipo",
                "dias_presentacion_garantia", "dias_reposicion",
                "anos_experiencia_minima", "pct_monto_referencia_minimo",
                "pct_monto_referencia_tecnica", "pct_interes_mora_diario",
                "pct_multa_diaria", "pct_diferencia_precio_bajo",
                "pct_diferencia_precio_alto", "pct_garantia_cumplimiento"]
    labels = ["num_categorias_docs", "num_referencias_min", "ref._técnicas_min",
              "pct_anticipo", "presentac._garantía", "dias_reposicion",
              "experiencia_mínima", "pct_monto_ref._min", "pct_monto_ref._técnica",
              "pct_int._mora_diario", "pct_multa_diaria", "pct_diff._precio_bajo",
              "pct_diff._precio_alto", "pct_garantía_cumpl."]
    acc_re = [df.loc[p, "acc_flex_r"] for p in portados]
    acc_llm = [max(df.loc[p, "acc_flex_l_raw"], df.loc[p, "acc_flex_l_pp"],
                   df.loc[p, "acc_flex_h"]) for p in portados]

    fig, ax = plt.subplots(figsize=(12.0, 6.4))
    x = np.arange(len(portados))
    w = 0.36
    b1 = ax.bar(x - w / 2, acc_llm, w, label="Mejor LLM anterior", color=RED,
                edgecolor="white")
    b2 = ax.bar(x + w / 2, acc_re, w, label="Regex portado", color=GREEN,
                edgecolor="white")
    for b, v in zip(b1, acc_llm):
        ax.text(b.get_x() + b.get_width() / 2, v + 1.2, f"{v:.0f}",
                ha="center", fontsize=8.0, color=GRAY)
    for b, v in zip(b2, acc_re):
        ax.text(b.get_x() + b.get_width() / 2, v + 1.2, f"{v:.0f}",
                ha="center", fontsize=8.0, fontweight="bold", color=GREEN)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.6, rotation=90)
    ax.set_title("14 campos portados de hybrid a regex  —  acierto (%) sobre GT humano")
    ax.set_ylabel("Acierto (%)")
    ax.set_ylim(0, 122)
    ax.legend(loc="upper left", frameon=False, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.text(7, 119.5, "Subidas de +25 a +88 pp · sin GPU · sin alucinación",
            ha="center", fontsize=9, color=DARK, fontweight="bold")
    fuente(ax, "Fuente: data/processed/alolab/comparativa_estrategias.csv (acc por campo)")
    save(fig, "portes_regex.png")


# =========================================================================
# 8. alolab_rondas.png
# =========================================================================
def chart_alolab():
    rondas = [1, 2, 3, 4]
    scores = [95, 95, 95, 100]
    fixes = ["3 hints +\nregex personal(?:es)?", "2 reglas\npostprocess +\nregla anticipo",
             "hint validez_garantia\n+ conv.\nmeses→días", "0 errores"]
    colors = [RED, ORANGE, ORANGE, GREEN]
    fig, ax = plt.subplots(figsize=(9.5, 6.0))
    bars = ax.bar([f"Ronda {r}" for r in rondas], scores, color=colors,
                  width=0.55, edgecolor="white")
    for b, v, f in zip(bars, scores, fixes):
        ax.text(b.get_x() + b.get_width() / 2, v + 2, f"{v}/100",
                ha="center", fontsize=12, fontweight="bold", color=DARK)
        ax.text(b.get_x() + b.get_width() / 2, 2, f, ha="center",
                fontsize=8.0, color=GRAY, va="bottom")
    ax.set_title("Proceso AloLab  —  precisión sobre 100 pliegos golden set humanos")
    ax.set_ylabel("Precisión (de 100)")
    ax.set_ylim(0, 118)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.annotate("objetivo logrado", xy=(2.78, 100.4), xytext=(2.0, 112.5),
                fontsize=9.5, color=GREEN, fontweight="bold", ha="center",
                arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5))
    icon(ax, 0.035, 0.94, "\uf013", size=18, color=GRAY, transform=ax.transAxes,
         ha="center", va="center")
    fuente(ax, "Fuente: scripts/alolab_collect_errors.py + errors_round{1..4}.json (Sesión 14)")
    save(fig, "alolab_rondas.png")


# =========================================================================
# 9. benchmark_modelos.png  — tabla horizontal: velocidad + precision + VRAM
# =========================================================================
def chart_benchmark():
    # (nombre, tiempo s, precision %, VRAM GiB, categoria, concurrencia × | None=n/d)
    # concurrencia = requests simultaneas sostenibles (KV cache, ctx 4096), medida en vLLM
    cat_col = {"marlin": GREEN, "awq": BLUE, "gguf": GRAY, "llamacpp": ORANGE}
    mods = [
        ("Qwen3.8-27B · llama.cpp (Win)", 1.41, 97, 14.0, "llamacpp", 1.0),
        ("Mistral 7B AWQ", 1.61, 100, 3.89, "marlin", 35.22),
        ("Llama 3.1 8B AWQ", 1.93, 100, 5.39, "marlin", 29.21),
        ("Qwen2.5 14B AWQ", 2.46, 100, 9.38, "awq", 8.18),
        ("Phi-4 14B AWQ", 4.47, 90, 8.51, "awq", 9.95),
        ("Qwen3-8B GGUF", 4.50, 100, 6.5, "gguf", None),
        ("Qwen3-8B AWQ", 4.80, 100, 5.71, "awq", None),
        ("Mistral 7B GGUF", 4.90, 100, 5.5, "gguf", 27.0),
        ("Qwen3-14B GGUF", 5.10, 100, 9.87, "gguf", 15.23),
        ("Qwen3-14B AWQ", 5.29, 100, 9.44, "awq", None),
        ("Phi-4 14B GGUF", 6.09, 100, 10.0, "gguf", None),
        ("Qwen2.5 14B GGUF", 7.10, 97.5, 10.55, "gguf", None),
    ]
    mods = sorted(mods, key=lambda m: m[1])
    names = [m[0] for m in mods]
    times = [m[1] for m in mods]
    accs = [m[2] for m in mods]
    vrams = [m[3] for m in mods]
    cols = [cat_col[m[4]] for m in mods]
    concs = [m[5] for m in mods]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14.6, 8.0), sharey=True,
                                   gridspec_kw={"width_ratios": [1.32, 1.0]})
    ypos = np.arange(len(mods))[::-1]

    # ---- Panel 1: latencia por muestra ----
    ax1.barh(ypos, times, height=0.62, color=cols, edgecolor="white")
    for y, t, a, v in zip(ypos, times, accs, vrams):
        ax1.text(t + 0.12, y, f"{t:.2f}s  ·  {a:g}%  ·  {v:.2f} GiB",
                 va="center", fontsize=9.3, color=DARK, fontweight="bold")
    ax1.set_yticks(ypos)
    ax1.set_yticklabels(names, fontsize=10.0)
    ax1.set_xlim(0, 9.6)
    ax1.set_title("Latencia por muestra (s)   [menor = mejor]", fontsize=12.5, pad=10)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.grid(axis="x", alpha=0.4)
    ax1.set_axisbelow(True)
    ax1.tick_params(axis="x", labelsize=10)
    ax1.axvline(3.0, ls="--", lw=1.0, color=GREEN, alpha=0.55)

    from matplotlib.patches import Patch
    handles = [Patch(color=cat_col["marlin"], label="AWQ · awq_marlin  (Mistral/Llama)"),
               Patch(color=cat_col["awq"], label="AWQ · kernel awq  (Qwen/Phi)"),
               Patch(color=cat_col["gguf"], label="GGUF  (deprecado)"),
               Patch(color=cat_col["llamacpp"], label="llama.cpp  (Windows)")]
    ax1.legend(handles=handles, loc="upper right", frameon=True, framealpha=1.0,
               edgecolor=BGRID, fontsize=9.0, borderpad=0.7, labelspacing=0.5)

    # ---- Panel 2: concurrencia (continuous batching) ----
    proj = {"Mistral 7B AWQ": "→ ≈4 h p/ 324K docs",
            "Llama 3.1 8B AWQ": "→ ≈5.8 h p/ 324K",
            "Mistral 7B GGUF": "→ ≈1 día",
            "Qwen3-14B GGUF": "→ ≈1.8 días"}
    for y, c, col, m in zip(ypos, concs, cols, mods):
        if c is None:
            ax2.text(0.7, y, "n/d", va="center", ha="left", fontsize=9.5,
                     color=GRAY, style="italic")
            continue
        est = (m[0] == "Mistral 7B GGUF")
        ax2.barh(y, c, height=0.62, color=col, edgecolor="white",
                 alpha=0.6 if est else 1.0)
        if m[0].startswith("Qwen3.8"):
            label = "1× secuencial  →  ≈5.3 días p/ 324K"
        else:
            label = f"{c:g}×" + (" (est.)" if est else "")
            if m[0] in proj:
                label += f"   {proj[m[0]]}"
        ax2.text(c + 0.5, y, label, va="center", fontsize=9.0,
                 color=DARK, fontweight="bold")
    ax2.set_xlim(0, 46)
    ax2.set_title("Concurrencia máx. — requests en paralelo   [mayor = mejor]",
                  fontsize=12.5, pad=10)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.grid(axis="x", alpha=0.4)
    ax2.set_axisbelow(True)
    ax2.tick_params(axis="x", labelsize=10)
    ax2.text(0.97, 0.05,
             "vLLM: continuous batching — N prompts\ncomparten la GPU en paralelo\n"
             "llama-server: 1 slot secuencial (sin batching)",
             transform=ax2.transAxes, ha="right", va="bottom", fontsize=8.8,
             color=DARK, linespacing=1.5,
             bbox=dict(boxstyle="round,pad=0.5", facecolor=LIGHT, edgecolor=BGRID, lw=1.0))

    fig.suptitle("Benchmark de modelos locales  —  latencia · concurrencia · precisión · VRAM  (GT humano)",
                 fontsize=15.5, fontweight="bold", y=0.975)
    fig.subplots_adjust(left=0.16, right=0.985, top=0.875, bottom=0.20)
    fig.text(0.012, 0.012,
             "Precisión = extracción estructurada correcta sobre GT humano  ·  F1 vs GT: Mistral 94.0 · Llama 96.4 · Qwen2.5 93.3\n"
             "Concurrencia = requests simultáneas sostenibles (KV cache, ctx 4096): vLLM agrupa prompts con continuous batching; llama-server atiende slots secuenciales\n"
             "Proyección fallback 324K docs: Mistral AWQ 35× → ~4 h · Llama AWQ 29× → ~5.8 h · Qwen3-14B GGUF 15× → ~1.8 días · Qwen3.8 llama.cpp 1× → ~5.3 días\n"
             "Excluidos por inviables: Gemma2 27B (OOM) · Llama GGUF (tokenizer gated) · Qwen2.5 7B (0%)  ·  n/d = no medido  ·  (est.) = estimado",
             fontsize=8.6, color=GRAY, va="bottom", linespacing=1.6)
    save(fig, "benchmark_modelos.png", tight=False)


# =========================================================================
# 10. proyeccion_fallback.png
# =========================================================================
def chart_proyeccion():
    esc = ["Baseline\nHTTP (servidor)", "Mistral GGUF\n(~27x conc.)",
           "Llama 3.1 8B AWQ\n(~5.8 h)", "Mistral 7B AWQ\n(~4 h)"]
    horas = [816, 24, 5.8, 4.0]
    colors = [RED, GRAY, GREEN, GREEN]
    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    bars = ax.bar(esc, horas, color=colors, width=0.6, edgecolor="white")
    for b, v in zip(bars, horas):
        lab = f"{v:.0f} días" if v >= 24 else f"{v:.1f} h"
        ax.text(b.get_x() + b.get_width() / 2, v * 1.15, lab,
                ha="center", fontsize=10.5, fontweight="bold", color=DARK)
    ax.set_yscale("log")
    ax.set_title("Proyección del fallback LLM  (~324K docs, 12 campos hybrid)  —  escala log")
    ax.set_ylabel("Tiempo estimado (horas, log)")
    ax.set_ylim(1, 1600)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.4)
    ax.set_axisbelow(True)
    ax.text(0.97, 0.96, "×200 más rápido\nvs baseline HTTP", transform=ax.transAxes,
            ha="right", va="top", fontsize=9.5, color=GREEN, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.35", facecolor="#EAF7EF", edgecolor=GREEN, lw=1.0))
    ax.add_patch(FancyArrowPatch((0.90, 0.62), (0.80, 0.10), arrowstyle="-|>",
                                 mutation_scale=12, linewidth=1.3, color=GREEN,
                                 transform=ax.transAxes))
    icon(ax, 0.035, 0.94, "\uf017", size=18, color=GRAY, transform=ax.transAxes,
         ha="center", va="center")
    fuente(ax, "Fuente: setup_ia_benchmark.md — continuous batching vLLM 0.23.0, WSL2")
    save(fig, "proyeccion_fallback.png")


# =========================================================================
# 10b. Imagenes individuales por etapa del ETL (detalle para exposicion)
# =========================================================================
def stage_header(ax, icon_code, tag, title, subtitle, color):
    p = FancyBboxPatch((2, 90), 96, 8.5, boxstyle="round,pad=0.3,rounding_size=1.2",
                       linewidth=0, facecolor=color, zorder=1)
    ax.add_patch(p)
    c = Circle((7, 94.25), 2.7, facecolor="white", edgecolor="none", zorder=3)
    ax.add_patch(c)
    icon(ax, 7, 94.25, icon_code, size=11.5, color=color, zorder=4)
    ax.text(11.2, 96.2, f"{tag}  ·  {title}", fontsize=14.5, fontweight="bold",
            color="white", ha="left", va="center", zorder=3)
    ax.text(11.2, 92.3, subtitle, fontsize=9.5, color="#DCE7F5", ha="left",
            va="center", zorder=3)


def big_card(ax, x, y, w, h, icon_code, title, body, color,
             title_size=11.5, body_size=8.6):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.5,rounding_size=1.4",
                       linewidth=2.0, edgecolor=color, facecolor="white", zorder=2)
    ax.add_patch(p)
    badge(ax, x + 5.2, y + h - 4.8, 3.0, icon_code, color, "white")
    ax.text(x + 10.2, y + h - 4.8, title, fontsize=title_size, fontweight="bold",
            color=DARK, ha="left", va="center", zorder=3)
    ax.plot([x + 3, x + w - 3], [y + h - 9.0, y + h - 9.0], color=BGRID, lw=1.0, zorder=2)
    ax.text(x + 3.5, y + h - 10.4, body, fontsize=body_size, color="#3E4854",
            ha="left", va="top", linespacing=1.6, zorder=3)


def stat_chip(ax, cx, cy, w, h, value, label, color):
    p = FancyBboxPatch((cx - w / 2, cy - h / 2), w, h,
                       boxstyle="round,pad=0.4,rounding_size=1.0",
                       linewidth=1.6, edgecolor=color, facecolor=LIGHT, zorder=2)
    ax.add_patch(p)
    ax.text(cx, cy + h * 0.17, value, ha="center", fontsize=13.5,
            fontweight="bold", color=color, zorder=3)
    ax.text(cx, cy - h * 0.24, label, ha="center", fontsize=8.2, color=GRAY, zorder=3)


def out_chip(ax, x, y, w, h, title, sub, color):
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.0",
                       linewidth=1.8, edgecolor=color, facecolor="white", zorder=2)
    ax.add_patch(p)
    ax.text(x + w / 2, y + h * 0.66, title, ha="center", fontsize=9.2,
            fontweight="bold", color=DARK, zorder=3)
    ax.text(x + w / 2, y + h * 0.26, sub, ha="center", fontsize=8.0,
            color=GRAY, zorder=3)


def stage_footer(ax, txt, y=3.2):
    ax.text(50, y, txt, ha="center", fontsize=9.0, color=GRAY)


def new_stage_fig():
    fig, ax = plt.subplots(figsize=(12.8, 7.6))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")
    return fig, ax


def chart_etl_fuente():
    fig, ax = new_stage_fig()
    stage_header(ax, "\uf1c0", "ETAPA 0", "FUENTES DE DATOS",
                 "Portal DNCP contrataciones.gov.py  ·  bulk oficial OCDS + pliegos PDF  ·  2021-2026", BLUE)
    big_card(ax, 4, 52, 44.5, 29, "\uf1c6", "Bulk OCDS · masivo_{year}.zip",
             "• 18 CSVs por año: records, awards, contracts,\n"
             "  tender, parties, items, planning, documents…\n"
             "• 73-462 MB por año  ·  ~1,283 MB en total\n"
             "• Datos estructurados: montos, fechas, oferentes,\n"
             "  ítems, método de contratación y protestas\n"
             "• Un corte oficial y reproducible por año", BLUE)
    big_card(ax, 51.5, 52, 44.5, 29, "\uf1c1", "PDFs de pliegos",
             "• Documento clave: 'Pliego Electrónico de Bases\n"
             "  y Condiciones' — texto NO estructurado\n"
             "• Filtro por tipo MIME: solo application/pdf\n"
             "• Whitelist: 31,387 pliegos electrónicos\n"
             "• Descarga propia: 32,785 PDFs (2021-2026)\n"
             "• Insumo del análisis de texto (etapa T)", ORANGE)
    box = FancyBboxPatch((4, 30.5), 92, 16.5, boxstyle="round,pad=0.4,rounding_size=1.2",
                         linewidth=1.4, edgecolor=BGRID, facecolor=LIGHT, zorder=2)
    ax.add_patch(box)
    icon(ax, 8.5, 43.2, "\uf05a", size=12, color=BLUE, zorder=3)
    ax.text(12, 43.2, "¿Por qué bulk y no API?", fontsize=10.5, fontweight="bold",
            color=DARK, va="center", zorder=3)
    ax.text(8.5, 39.6, "El snapshot masivo garantiza el mismo corte de datos para las 17 tablas y evita los límites de tasa por request de la API.\n"
                       "Los PDFs viajan fuera del bulk: se descubren en los metadatos OCDS (tender.documents) y se descargan en paralelo.",
            fontsize=8.8, color="#3E4854", va="top", linespacing=1.6, zorder=3)
    for cx, v, l, c in [(15, "6", "años de datos", BLUE), (38.3, "18", "CSVs por año", BLUE),
                        (61.6, "31,387", "pliegos en whitelist", ORANGE), (85, "32,785", "PDFs descargados", ORANGE)]:
        stat_chip(ax, cx, 15.5, 20, 11, v, l, c)
    stage_footer(ax, "Los dos mundos (tablas OCDS + texto de pliegos) se unen por nro_licitacion  ·  Pipeline ETL — etapa 0 de 5")
    save(fig, "etl_1_fuente.png")


def chart_etl_extraccion():
    fig, ax = new_stage_fig()
    stage_header(ax, "\uf5fd", "ETAPA E", "EXTRACCIÓN",
                 "del ZIP y los PDFs crudos a secciones limpias  ·  Apache Tika + PyPDF2 + asyncio", GREEN)
    cards = [
        (4, 63, "\uf1c6", "1 · Descomprimir",
         "masivo_{year}.zip → 18 CSVs\npor año (pandas, lectura\nselectiva de columnas)"),
        (35, 63, "\uf0b0", "2 · Filtrar pliegos",
         "tender.documents = pliego\nelectrónico (application/pdf)\n→ whitelist 31,387"),
        (66, 63, "\uf019", "3 · Descargar PDFs",
         "asyncio + aiohttp en batches\nde 1,000 · con reintentos\n→ 32,785 PDFs"),
        (4, 39, "\uf15c", "4 · Texto + outlines",
         "Tika (Podman): texto línea\na línea + nº de página\nPyPDF2: bookmarks prof. 1-2"),
        (35, 39, "\uf5fd", "5 · Cruce → secciones",
         "outlines × líneas de Tika\n5 casos de límite resueltos\ndedup de índice → 2.78M"),
        (66, 39, "\uf12d", "6 · Limpiar",
         "guiones de fin de línea ·\nnúmeros de página · Unicode\nNFKC · stemming español"),
    ]
    for x, y, ic, t, b in cards:
        big_card(ax, x, y, 30, 21, ic, t, b, GREEN, title_size=10.2, body_size=8.2)
    for xa in (19, 50, 81):
        varrow(ax, xa, 62.7, xa, 60.3, GREEN, lw=1.4)
        varrow(ax, xa, 38.7, xa, 33.9, GREEN, lw=1.4)
    box = FancyBboxPatch((18, 25), 64, 8.5, boxstyle="round,pad=0.4,rounding_size=1.2",
                         linewidth=1.8, edgecolor=GREEN, facecolor="#EAF7EF", zorder=2)
    ax.add_patch(box)
    icon(ax, 23, 29.25, "\uf5fd", size=11, color=GREEN, zorder=3)
    ax.text(27, 31.1, "Resultado de la etapa E", fontsize=9.5, fontweight="bold",
            color=GREEN, va="center", zorder=3)
    ax.text(27, 27.3, "pliegos_secciones · ~2.78M filas · Parquet por año · checkpoint .sections_db_done_{year}",
            fontsize=8.6, color=DARK, va="center", zorder=3)
    for cx, v, l in [(20, "31,387", "en whitelist"), (50, "32,785", "PDFs descargados"),
                     (80, "2.78M", "secciones limpias")]:
        stat_chip(ax, cx, 14.5, 24, 11, v, l, GREEN)
    stage_footer(ax, "Cada año es re-ejecutable de forma idempotente gracias a los checkpoints  ·  Pipeline ETL — etapa E de 5")
    save(fig, "etl_2_extraccion.png")


def chart_etl_transformacion():
    fig, ax = new_stage_fig()
    stage_header(ax, "\uf013", "ETAPA T", "TRANSFORMACIÓN",
                 "81 campos por licitación  ·  regex determinista + LLM híbrido con fallback", PURPLE)
    big_card(ax, 4, 51, 44.5, 30, "\uf013", "Regex determinista · 69 campos",
             "• Patrones sobre plantillas del pliego\n"
             "  (pct_*, días, conteos, binarios)\n"
             "• Cada patrón validado contra GT humano\n"
             "  (26 campos · 5,066 juicios)\n"
             "• ~70 s de CPU para 31,321 docs · sin GPU\n"
             "• Confianza por campo: {campo}_conf", BLUE)
    big_card(ax, 51.5, 51, 44.5, 30, "\uf5dc", "LLM híbrido (fallback) · 12 campos",
             "• Solo docs donde regex falló (conf < 0.8)\n"
             "  → ~324K documentos pendientes\n"
             "• Llama 3.1 8B AWQ · vLLM · guided decoding\n"
             "• Postproceso determinista compartido\n"
             "  (meses→días, reglas anti-falsos positivos)\n"
             "• AloLab: 100/100 sobre 100 GT humanos", PURPLE)
    varrow(ax, 26.25, 50.7, 26.25, 39.6, PURPLE)
    varrow(ax, 73.75, 50.7, 73.75, 39.6, PURPLE)
    out_chip(ax, 4, 26, 29, 13, "document_features_v2", "81 campos · 31,321 docs", PURPLE)
    out_chip(ax, 35.5, 26, 29, 13, "document_economic_features", "46 cols económicas · 29,811 docs", PURPLE)
    out_chip(ax, 67, 26, 29, 13, "document_features", "571 cols estructurales · 31,321 docs", PURPLE)
    box = FancyBboxPatch((4, 10), 92, 10, boxstyle="round,pad=0.4,rounding_size=1.2",
                         linewidth=1.6, edgecolor=PURPLE, facecolor="#F6EFFA", zorder=2)
    ax.add_patch(box)
    icon(ax, 8.7, 15, "\uf201", size=11, color=PURPLE, zorder=3)
    ax.text(12.5, 16.6, "Comparativa sobre GT humano (26 campos · 5,066 juicios):  híbrido H = 68.3  >  regex R = 67.0  >  LLM+postproceso = 63.1",
            fontsize=8.8, color=DARK, va="center", zorder=3)
    ax.text(12.5, 13.2, "El postproceso determinista suma +12 pp sobre el LLM bruto  ·  los 5 campos pct de plantilla quedan al 100% con regex puro",
            fontsize=8.8, color=GRAY, va="center", zorder=3)
    stage_footer(ax, "81 campos = 69 regex + 12 hybrid (3 irresolubles permanecen hybrid)  ·  Pipeline ETL — etapa T de 5")
    save(fig, "etl_3_transformacion.png")


def chart_etl_carga():
    fig, ax = new_stage_fig()
    stage_header(ax, "\uf1c0", "ETAPA L", "CARGA",
                 "base de datos PostgreSQL dncp  ·  cargas por lotes e idempotentes", BLUE)
    box = FancyBboxPatch((22, 76.5), 56, 9, boxstyle="round,pad=0.4,rounding_size=1.2",
                         linewidth=0, facecolor=BLUE, zorder=2)
    ax.add_patch(box)
    icon(ax, 28, 81, "\uf1c0", size=13, color="white", zorder=3)
    ax.text(33, 81, "Base de datos  dncp  ·  PostgreSQL 16 (Podman)", fontsize=12,
            fontweight="bold", color="white", va="center", zorder=3)
    varrow(ax, 26.25, 76.2, 26.25, 68.6, BLUE)
    varrow(ax, 73.75, 76.2, 73.75, 68.6, BLUE)
    big_card(ax, 4, 42, 44.5, 26, "\uf1c0", "OCDS estructurado · 17 tablas",
             "• Carga en orden de claves foráneas:\n"
             "  convocantes → tender → items →\n"
             "  awards → contracts → protestas\n"
             "• psycopg2 raw (sin ORM)\n"
             "• execute_values por lotes", BLUE, body_size=8.4)
    big_card(ax, 51.5, 42, 44.5, 26, "\uf5fd", "pliegos_secciones · 2.78M filas",
             "• Particionada por año\n"
             "• COPY en batches de 10,000 filas\n"
             "• ON CONFLICT DO NOTHING (idempotente)\n"
             "• Checkpoint por año:\n"
             "  .sections_db_done_{year}", GREEN, body_size=8.4)
    varrow(ax, 26.25, 41.7, 26.25, 35.6, BLUE, lw=1.4)
    varrow(ax, 73.75, 41.7, 73.75, 35.6, BLUE, lw=1.4)
    out_chip(ax, 4, 22, 29, 13, "document_features_v2", "81 campos · 31,321 docs", PURPLE)
    out_chip(ax, 35.5, 22, 29, 13, "document_economic_features", "46 cols · 29,811 docs", PURPLE)
    out_chip(ax, 67, 22, 29, 13, "document_features", "571 cols · 31,321 docs", PURPLE)
    for cx, v, l in [(20, "17", "tablas OCDS"), (50, "2.78M", "filas de secciones"),
                     (80, "3", "matrices de features")]:
        stat_chip(ax, cx, 12.5, 24, 11, v, l, BLUE)
    stage_footer(ax, "Todo el pipeline queda consultable en SQL para el algoritmo  ·  Pipeline ETL — etapa L de 5", y=2.8)
    save(fig, "etl_4_carga.png")


def chart_etl_algoritmo():
    fig, ax = new_stage_fig()
    stage_header(ax, "\uf024", "SIGUIENTE ETAPA", "APLICACIÓN DEL ALGORITMO",
                 "detección de anomalías  ·  candidatos evaluables — decisión pendiente", RED)
    box = FancyBboxPatch((4, 71), 92, 15, boxstyle="round,pad=0.4,rounding_size=1.2",
                         linewidth=1.6, edgecolor=RED, facecolor="#FDECEA", zorder=2)
    ax.add_patch(box)
    ax.text(7.5, 82.6, "Insumos ya calculados", fontsize=10.5, fontweight="bold",
            color=RED, va="center", zorder=3)
    ax.text(7.5, 79.6, "• Red flags por licitación: unicidad de oferente · overbudget_ratio · recurrencia del ganador · urgencia de compra\n"
                       "• Ranking ponderado (6 estrategias): outlier 0.20 · random forest 0.25 · contextual 0.15 · section_if 0.15 · correlación 0.10 · económica 0.15\n"
                       "• Validación externa disponible: 1,253 protestas en 967 licitaciones (4%)",
            fontsize=8.4, color=DARK, va="top", linespacing=1.65, zorder=3)
    cands = [
        (4, 46, "\uf1e0", "A · Ensemble ISF/LOF/ECOD", "no supervisado sobre features\npor licitación (ya calculadas)\npuntúa y explica (DIFFI/SHAP)", "viable", GREEN),
        (35, 46, "\uf0e4", "B · Red flags: CRI + Benford", "índice compuesto estilo FMI +\nley de Benford: 1.87M ítems ·\n265K pagos (replicación PY)", "viable", GREEN),
        (66, 46, "\uf3c5", "C · Screens econométricos", "distancia ganador → referencial\n+ screens de recurrencia\n(sin montos por oferente)", "parcial", ORANGE),
        (4, 21.5, "\uf0e8", "D · Red de co-oferta", "grafo de co-oferta: 5,544\noferentes · 78,695 ofertas\n(enfoque Wachs)", "viable", GREEN),
        (35, 21.5, "\uf071", "E · Supervisado (protestas)", "1,253 protestas como etiqueta\nproxy · RF/GBM con screens\n(literatura: 77-91%)", "proxy 4%", ORANGE),
        (66, 21.5, "\uf15c", "F · NLP de pliegos", "texto del pliego →\nes_unico_oferente (Katona)\nel pipeline v2 ya preprocesa", "preproceso listo", ORANGE),
    ]
    for x, y, ic, t, b, tag, tc in cands:
        big_card(ax, x, y, 30, 22, ic, t, b, tc, title_size=9.8, body_size=7.9)
        ax.text(x + 30 - 1.8, y + 22 - 2.2, tag, fontsize=7.2, fontweight="bold",
                color=tc, ha="right", va="center", zorder=4)
    ax.text(6, 18.2, "Criterios de decisión (a definir con tutores):", fontsize=8.5,
            color=GRAY, style="italic", va="center")
    for cx, l in [(22, "Precisión"), (43, "Explicabilidad"), (66, "Novedad académica"), (87, "Costo de GPU")]:
        p = FancyBboxPatch((cx - 9.5, 9.5), 19, 7, boxstyle="round,pad=0.35,rounding_size=1.0",
                           linewidth=1.5, edgecolor=GRAY, facecolor=LIGHT, zorder=2)
        ax.add_patch(p)
        ax.text(cx, 13, l, ha="center", fontsize=8.8, fontweight="bold", color=DARK, zorder=3)
    stage_footer(ax, "Hoja de ruta sugerida: Benford → CRI → ISF/ECOD → ensemble con explicabilidad → red de co-oferta → supervisado con protestas", y=4.2)
    save(fig, "etl_5_algoritmo.png")


if __name__ == "__main__":
    chart_flujo()
    chart_etl_fuente()
    chart_etl_extraccion()
    chart_etl_transformacion()
    chart_etl_carga()
    chart_etl_algoritmo()
    chart_zips()
    chart_pdfs()
    chart_volumenes()
    chart_campos()
    chart_estrategias()
    chart_portes()
    chart_alolab()
    chart_benchmark()
    chart_proyeccion()
    print("\nListo — graficas en:", OUT)
