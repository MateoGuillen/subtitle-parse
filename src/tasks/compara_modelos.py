"""
Comparación de resultados de LLMs con ground truth"""

import json
import re
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG


# ----------------------------
# Normalización de valores
# ----------------------------
def normalize_value(v):
    """Normaliza valores para comparación robusta"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str):
        val = v.strip().lower()
        # Si parece numérico, lo convierto
        try:
            return round(float(val), 3)
        except ValueError:
            return val
    if isinstance(v, (int, float)):
        return round(float(v), 3)
    return v


# ----------------------------
# Extraer base del title_slug
# ----------------------------
def get_base_slug(title_slug):
    """
    Extrae el slug base removiendo sufijos como _v2, _v3, _ground_truth
    Ej: apertura_de_ofertas_v3 -> apertura_de_ofertas
    """

    # Remover sufijos comunes: _v1, _v2, _v3, etc. y _ground_truth
    base = re.sub(r"_(v\d+|ground_truth)$", "", title_slug)
    return base


# ----------------------------
# Conectar a PostgreSQL
# ----------------------------
def fetch_results():
    """Obtiene resultados de la base de datos"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Traer todos los resultados
        cur.execute(
            """
            SELECT nro_licitacion, title_slug, modelo, json_result
            FROM dncp.llm_resultados
            WHERE modelo IN (
                'ground_truth_humano',
                'deepseek-r1-distill-llama-8b',
                'qwen2.5-7b-instruct',
                'openai/gpt-oss-20b',
                'google/gemma-3-12b',
                'qwen/qwen3-8b'
            )
            ORDER BY nro_licitacion, title_slug, modelo
            """
        )

        rows = cur.fetchall()
        cur.close()

        df = pd.DataFrame(
            rows, columns=["nro_licitacion", "title_slug", "modelo", "json_result"]
        )

        # Agregar columna con el slug base
        df["base_slug"] = df["title_slug"].apply(get_base_slug)

        # Normalizar json_result: siempre dict
        def normalize_json(x):
            if x is None or pd.isna(x):
                return {}
            if isinstance(x, dict):
                return x
            if isinstance(x, str):
                try:
                    return json.loads(x)
                except json.JSONDecodeError:
                    return {}
            return {}

        df["json_result"] = df["json_result"].apply(normalize_json)
        return df

    except psycopg2.Error as e:
        print(f"Error de base de datos: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error inesperado: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


# ----------------------------
# Comparar contra ground truth
# ----------------------------
def compare_with_ground_truth(df):
    """Compara cada modelo contra el ground truth"""
    if df.empty:
        return pd.DataFrame()

    # Verificar que existe ground_truth_humano
    ground_truth_df = df[df["modelo"] == "ground_truth_humano"].copy()

    if ground_truth_df.empty:
        print("Advertencia: No se encontró 'ground_truth_humano' en los datos")
        return pd.DataFrame()

    # Obtener modelos a comparar
    modelos_df = df[df["modelo"] != "ground_truth_humano"].copy()

    if modelos_df.empty:
        print("No hay modelos para comparar")
        return pd.DataFrame()

    results = []

    # Para cada ground truth
    for _, gt_row in ground_truth_df.iterrows():
        nro_lic = gt_row["nro_licitacion"]
        base_slug = gt_row["base_slug"]
        gt_json = gt_row["json_result"]

        if not isinstance(gt_json, dict) or not gt_json:
            continue

        # Buscar todos los modelos con el mismo nro_licitacion y base_slug
        modelos_match = modelos_df[
            (modelos_df["nro_licitacion"] == nro_lic)
            & (modelos_df["base_slug"] == base_slug)
        ]

        for _, modelo_row in modelos_match.iterrows():
            pred_json = modelo_row["json_result"]

            if not isinstance(pred_json, dict):
                pred_json = {}

            correct = 0
            total = len(gt_json)
            mismatches = []

            for key, gt_value in gt_json.items():
                gt_norm = normalize_value(gt_value)
                pred_norm = normalize_value(pred_json.get(key))

                if pred_norm == gt_norm:
                    correct += 1
                else:
                    mismatches.append(
                        {
                            "campo": key,
                            "ground_truth": gt_value,
                            "prediccion": pred_json.get(key, "FALTANTE"),
                        }
                    )

            accuracy = correct / total if total > 0 else 0

            results.append(
                {
                    "nro_licitacion": nro_lic,
                    "base_slug": base_slug,
                    "title_slug_gt": gt_row["title_slug"],
                    "title_slug_modelo": modelo_row["title_slug"],
                    "modelo": modelo_row["modelo"],
                    "accuracy": accuracy,
                    "correct": correct,
                    "total": total,
                    "mismatches": (
                        json.dumps(mismatches, ensure_ascii=False) if mismatches else ""
                    ),
                }
            )

    return pd.DataFrame(results)


# ----------------------------
# Guardar resultados
# ----------------------------
def save_results(comparison):
    """Guarda los resultados en CSV y Excel"""
    try:
        # Guardar CSV detallado
        comparison.to_csv("comparacion_detallada.csv", index=False, encoding="utf-8")
        print("✓ Archivo 'comparacion_detallada.csv' guardado exitosamente")

        # Guardar Excel con dos hojas
        with pd.ExcelWriter("comparacion.xlsx", engine="openpyxl") as writer:
            # Hoja 1: Datos detallados
            comparison.to_excel(writer, sheet_name="Detalle", index=False)

            # Hoja 2: Resumen por modelo
            summary = (
                comparison.groupby("modelo")
                .agg(
                    {
                        "accuracy": ["mean", "std", "min", "max"],
                        "nro_licitacion": "count",
                    }
                )
                .round(4)
            )
            summary.columns = [
                "Accuracy Promedio",
                "Desv. Estándar",
                "Min",
                "Max",
                "Num. Comparaciones",
            ]
            summary = summary.sort_values("Accuracy Promedio", ascending=False)
            summary.to_excel(writer, sheet_name="Resumen")

        print("✓ Archivo 'comparacion.xlsx' guardado exitosamente")

    except Exception as e:
        print(f"Error al guardar archivos: {e}")


# ----------------------------
# Mostrar resumen de resultados
# ----------------------------
def show_summary(comparison):
    """Muestra un resumen de los resultados"""
    if comparison.empty:
        return

    print("\n" + "=" * 80)
    print("RESUMEN DE ACCURACY POR MODELO")
    print("=" * 80)

    summary = (
        comparison.groupby("modelo")
        .agg({"accuracy": ["mean", "std", "min", "max"], "nro_licitacion": "count"})
        .round(4)
    )

    summary.columns = ["Promedio", "Desv.Std", "Mínimo", "Máximo", "N° Comparaciones"]
    summary = summary.sort_values("Promedio", ascending=False)

    print(summary.to_string())
    print("=" * 80)

    # Mostrar top y bottom performers
    print("\n📊 MEJORES RESULTADOS (Top 6):")
    top_results = comparison.nlargest(6, "accuracy")[
        ["nro_licitacion", "base_slug", "modelo", "accuracy", "correct", "total"]
    ]
    print(top_results.to_string(index=False))

    print("\n📉 PEORES RESULTADOS (Bottom 6):")
    bottom_results = comparison.nsmallest(6, "accuracy")[
        ["nro_licitacion", "base_slug", "modelo", "accuracy", "correct", "total"]
    ]
    print(bottom_results.to_string(index=False))
    print()


# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    print("🚀 Iniciando comparación de modelos...")

    # 1. Obtener datos
    df = fetch_results()

    if df.empty:
        print("❌ No se encontraron resultados en la tabla.")
    else:
        print(f"✓ Se obtuvieron {len(df)} registros de la base de datos")
        print(f"  - Ground truths: {len(df[df['modelo'] == 'ground_truth_humano'])}")
        print(
            f"  - Predicciones de modelos: {len(df[df['modelo'] != 'ground_truth_humano'])}"
        )

        # 2. Comparar con ground truth
        comparison = compare_with_ground_truth(df)

        if comparison.empty:
            print("❌ No se pudo generar la comparación")
        else:
            print(f"✓ Se generaron {len(comparison)} comparaciones")

            # 3. Mostrar resumen
            show_summary(comparison)

            # 4. Guardar resultados
            save_results(comparison)

            # 5. Mostrar muestra de datos
            print("\n📋 MUESTRA DE COMPARACIONES (primeras 10 filas):")
            display_cols = [
                "nro_licitacion",
                "base_slug",
                "modelo",
                "accuracy",
                "correct",
                "total",
            ]
            print(comparison[display_cols].head(10).to_string(index=False))
