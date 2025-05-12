import pandas as pd
from tika import parser
import os
from pathlib import Path

# Configuración (ajusta según tu estructura)
years = [2021]
from config.settings import BASE_OUTPUT_RAW_DIR, BASE_OUTPUT_PROCESSED_DIR

output_processed_dir = f"{BASE_OUTPUT_PROCESSED_DIR}/parquet/pdf-to-parquet/"


def pdf_to_parquet(pdf_path, output_dir=output_processed_dir):
    """
    Convierte un PDF a Parquet, dividiendo el contenido por líneas

    Args:
        pdf_path (str): Ruta al archivo PDF de entrada
        output_dir (str): Directorio de salida para el Parquet
    """
    # Asegurar que existe el directorio de salida
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Extraer texto del PDF usando Tika
    print(f"Procesando PDF: {pdf_path}")
    parsed = parser.from_file(pdf_path)
    text_content = parsed["content"]

    if not text_content:
        print("Advertencia: No se pudo extraer texto del PDF")
        return

    # Dividir por líneas y limpiar
    lines = [line.strip() for line in text_content.split("\n") if line.strip()]

    # Crear DataFrame
    df = pd.DataFrame(
        {
            "line_number": range(1, len(lines) + 1),
            "text": lines,
            "source_file": os.path.basename(pdf_path),
        }
    )

    # Generar nombre de archivo de salida
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{pdf_name}.parquet")

    # Guardar como Parquet
    df.to_parquet(output_path)
    print(f"Archivo Parquet guardado en: {output_path}")

    return output_path


# Ejemplo de uso
if __name__ == "__main__":
    # Cambia esto por la ruta a tu PDF
    # sample_pdf = f"{BASE_OUTPUT_RAW_DIR}/pdf/test_pdfs/2021_1_395227.pdf"  # o usa BASE_OUTPUT_RAW_DIR si es apropiado
    sample_pdf = f"{BASE_OUTPUT_RAW_DIR}/pdf/test_pdfs/2022_1_390672.pdf"  # o usa BASE_OUTPUT_RAW_DIR si es apropiado

    # Ejecutar la conversión
    pdf_to_parquet(sample_pdf)
