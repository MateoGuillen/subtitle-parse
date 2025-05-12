"""
Script para comparar diferentes métodos de extracción de texto de PDFs y
exportar los resultados a un archivo CSV para análisis detallado.
"""

import csv
import asyncio
import io
from pathlib import Path
from typing import List, Dict, Set, Tuple
import re

# Importar las funciones del módulo original
from PyPDF2 import PdfReader
import tika
from tika import parser
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

# Inicializar Tika
tika.initVM()


@dataclass
class PDFSection:
    """Data structure for a text line in a PDF"""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[str] = None


def extract_with_pypdf2(pdf_bytes: bytes, filename: str) -> List[PDFSection]:
    """
    Extrae texto de un PDF usando PyPDF2, que preserva mejor algunos caracteres especiales.
    """
    sections = []
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for page_number, page in enumerate(reader.pages, start=1):
            text = page.extract_text()
            if not text:
                continue

            lines = text.split("\n")
            for line_number, line in enumerate(lines, start=1):
                clean_line = line.strip()
                if clean_line and not re.fullmatch(r"\d{1,3}/\d{1,3}", clean_line):
                    sections.append(
                        PDFSection(
                            document_id=filename,
                            page_number=page_number,
                            line_number=line_number,
                            line_text=clean_line,
                            processed_date=datetime.now().isoformat(),
                        )
                    )
    except Exception as e:
        print(f"Error processing PDF with PyPDF2: {e}")

    return sections


def extract_with_tika(pdf_bytes: bytes, filename: str) -> List[PDFSection]:
    """
    Extrae texto de un PDF usando Apache Tika, que es muy robusto para diferentes
    tipos de PDFs y preserva mejor caracteres especiales.
    """
    sections = []
    try:
        parsed = parser.from_buffer(pdf_bytes)
        if parsed["content"]:
            text = parsed["content"]
            lines = text.split("\n")

            page_number = 1
            line_number = 0

            for line in lines:
                line = line.strip()

                # Detectar cambios de página por patrones comunes
                if (
                    re.match(r"^Page \d+$", line)
                    or re.match(r"^\d+$", line)
                    and len(line) < 5
                ):
                    page_number += 1
                    line_number = 0
                    continue

                # Filtrar líneas que son solo números de página
                if not line or re.fullmatch(r"\d{1,3}/\d{1,3}", line):
                    continue

                line_number += 1
                sections.append(
                    PDFSection(
                        document_id=filename,
                        page_number=page_number,
                        line_number=line_number,
                        line_text=line,
                        processed_date=datetime.now().isoformat(),
                    )
                )
    except Exception as e:
        print(f"Error processing PDF with Tika: {e}")

    return sections


def create_page_text_map(sections: List[PDFSection]) -> Dict[int, str]:
    """
    Crea un diccionario que mapea números de página al texto completo de esa página.
    Esto permite una mejor comparación entre diferentes extractores.
    """
    page_map = {}
    for section in sections:
        if section.page_number not in page_map:
            page_map[section.page_number] = []

        page_map[section.page_number].append(section.line_text)

    # Convertir las listas de líneas a texto completo por página
    return {page: "\n".join(lines) for page, lines in page_map.items()}


async def compare_and_export_csv(pdf_path: Path, output_csv_path: Path = None):
    """
    Compara los resultados de diferentes extractores de PDF y exporta la comparación a un CSV.

    El CSV tendrá una estructura donde cada fila representa una línea extraída,
    con columnas para cada extractor, mostrando lo que extrajeron para esa línea.
    """
    pdf_bytes = pdf_path.read_bytes()
    filename = pdf_path.stem

    # Si no se proporciona una ruta de salida, usar el mismo nombre con extensión .csv
    if output_csv_path is None:
        output_csv_path = pdf_path.parent / f"{filename}_comparison.csv"

    # Extraer con cada método
    pypdf2_sections = extract_with_pypdf2(pdf_bytes, filename)
    tika_sections = extract_with_tika(pdf_bytes, filename)

    # Crear una estructura para comparar línea por línea
    comparison_data = []

    # Obtener todos los números de página únicos
    pypdf2_pages = {s.page_number for s in pypdf2_sections}
    tika_pages = {s.page_number for s in tika_sections}
    all_pages = sorted(pypdf2_pages.union(tika_pages))

    # Para cada página, obtener todas las líneas y compararlas
    for page in all_pages:
        # Filtrar secciones por página
        pypdf2_page_sections = [s for s in pypdf2_sections if s.page_number == page]
        tika_page_sections = [s for s in tika_sections if s.page_number == page]

        # Ordenar por número de línea
        pypdf2_page_sections.sort(key=lambda s: s.line_number)
        tika_page_sections.sort(key=lambda s: s.line_number)

        # Determinar el número máximo de líneas para esta página
        max_lines = max(len(pypdf2_page_sections), len(tika_page_sections))

        # Crear una entrada para cada línea
        for i in range(max_lines):
            pypdf2_text = (
                pypdf2_page_sections[i].line_text
                if i < len(pypdf2_page_sections)
                else ""
            )
            tika_text = (
                tika_page_sections[i].line_text if i < len(tika_page_sections) else ""
            )

            comparison_data.append(
                {"page": page, "line": i + 1, "pypdf2": pypdf2_text, "tika": tika_text}
            )

    # Exportar a CSV
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["page", "line", "pypdf2", "tika"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in comparison_data:
            writer.writerow(row)

    print(f"Comparación guardada en: {output_csv_path}")
    print(f"Total de líneas comparadas: {len(comparison_data)}")
    print(f"PyPDF2: {len(pypdf2_sections)} líneas")
    print(f"Tika: {len(tika_sections)} líneas")

    return comparison_data


# Método alternativo que compara texto completo página por página
async def compare_by_page_content(pdf_path: Path, output_csv_path: Path = None):
    """
    Compara el contenido completo de cada página extraído por diferentes métodos.
    Este enfoque es útil cuando la estructura de líneas puede variar entre extractores.
    """
    pdf_bytes = pdf_path.read_bytes()
    filename = pdf_path.stem

    # Si no se proporciona una ruta de salida, usar el mismo nombre con extensión .csv
    if output_csv_path is None:
        output_csv_path = pdf_path.parent / f"{filename}_page_comparison.csv"

    # Extraer con cada método
    pypdf2_sections = extract_with_pypdf2(pdf_bytes, filename)
    tika_sections = extract_with_tika(pdf_bytes, filename)

    # Crear mapas de página a texto
    pypdf2_page_map = create_page_text_map(pypdf2_sections)
    tika_page_map = create_page_text_map(tika_sections)

    # Obtener todos los números de página únicos
    all_pages = sorted(set(pypdf2_page_map.keys()).union(set(tika_page_map.keys())))

    # Preparar datos para CSV
    csv_rows = []
    for page in all_pages:
        pypdf2_text = pypdf2_page_map.get(page, "")
        tika_text = tika_page_map.get(page, "")

        csv_rows.append({"page": page, "pypdf2": pypdf2_text, "tika": tika_text})

    # Escribir al CSV
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["page", "pypdf2", "tika"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    print(f"Comparación por página guardada en: {output_csv_path}")
    print(f"Total de páginas comparadas: {len(csv_rows)}")

    return csv_rows


# Enfoque mejorado: comparar línea por línea con mejor alineación
async def compare_lines_improved(pdf_path: Path, output_csv_path: Path = None):
    """
    Compara líneas de texto extraídas por diferentes métodos,
    intentando alinear mejor las líneas para una comparación más precisa.
    """
    pdf_bytes = pdf_path.read_bytes()
    filename = pdf_path.stem

    # Si no se proporciona una ruta de salida, usar el mismo nombre con extensión .csv
    if output_csv_path is None:
        output_csv_path = pdf_path.parent / f"{filename}_line_comparison.csv"

    # Extraer con cada método
    pypdf2_sections = extract_with_pypdf2(pdf_bytes, filename)
    tika_sections = extract_with_tika(pdf_bytes, filename)

    # Crear una estructura para los resultados por página
    results = []

    # Obtener todos los números de página únicos
    pypdf2_pages = {s.page_number for s in pypdf2_sections}
    tika_pages = {s.page_number for s in tika_sections}
    all_pages = sorted(pypdf2_pages.union(tika_pages))

    line_counter = 1  # Contador global de líneas para el CSV

    # Para cada página, procesar las líneas
    for page in all_pages:
        # Filtrar secciones por página
        pypdf2_lines = [s.line_text for s in pypdf2_sections if s.page_number == page]
        tika_lines = [s.line_text for s in tika_sections if s.page_number == page]

        # Si una de las listas está vacía, simplemente añadir las líneas de la otra
        if not pypdf2_lines:
            for tika_line in tika_lines:
                results.append(
                    {
                        "global_line": line_counter,
                        "page": page,
                        "pypdf2": "",
                        "tika": tika_line,
                    }
                )
                line_counter += 1
            continue

        if not tika_lines:
            for pypdf2_line in pypdf2_lines:
                results.append(
                    {
                        "global_line": line_counter,
                        "page": page,
                        "pypdf2": pypdf2_line,
                        "tika": "",
                    }
                )
                line_counter += 1
            continue

        # Si ambos extractores tienen líneas, intentar alinearlas
        # Usamos un enfoque simple: el extractor con más líneas establece el ritmo
        max_lines = max(len(pypdf2_lines), len(tika_lines))

        for i in range(max_lines):
            pypdf2_text = pypdf2_lines[i] if i < len(pypdf2_lines) else ""
            tika_text = tika_lines[i] if i < len(tika_lines) else ""

            results.append(
                {
                    "global_line": line_counter,
                    "page": page,
                    "pypdf2": pypdf2_text,
                    "tika": tika_text,
                }
            )
            line_counter += 1

    # Escribir al CSV
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["global_line", "page", "pypdf2", "tika"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"Comparación línea por línea guardada en: {output_csv_path}")
    print(f"Total de líneas comparadas: {len(results)}")
    print(f"PyPDF2: {len(pypdf2_sections)} líneas totales")
    print(f"Tika: {len(tika_sections)} líneas totales")

    return results


# Función principal para ejecutar todos los métodos de comparación
async def run_all_comparisons(pdf_path: Path, output_dir: Path = None):
    """
    Ejecuta todas las comparaciones disponibles y guarda los resultados
    en archivos CSV separados.
    """
    if output_dir is None:
        output_dir = pdf_path.parent

    filename = pdf_path.stem

    # Asegurarse de que el directorio de salida existe
    output_dir.mkdir(parents=True, exist_ok=True)

    # Ejecutar cada método de comparación
    await compare_and_export_csv(
        pdf_path, output_dir / f"{filename}_basic_comparison.csv"
    )

    await compare_by_page_content(
        pdf_path, output_dir / f"{filename}_page_comparison.csv"
    )

    await compare_lines_improved(
        pdf_path, output_dir / f"{filename}_line_comparison.csv"
    )

    print(f"Todas las comparaciones completadas y guardadas en: {output_dir}")


# Ejemplo de uso
if __name__ == "__main__":
    # Importar la configuración si está disponible, o usar un valor por defecto
    try:
        from config.settings import BASE_OUTPUT_RAW_DIR

        pdf_dir = f"{BASE_OUTPUT_RAW_DIR}/pdf/test_pdfs"
    except ImportError:
        # Si no se puede importar la configuración, usar un directorio relativo
        pdf_dir = "./test_pdfs"

    async def main():
        """Main function to compare extractors and export to CSV"""
        # Puedes especificar un PDF o procesar todos los PDFs en un directorio
        pdf_path = Path(f"{pdf_dir}/2021_1_395227.pdf")

        # Para un solo PDF
        await run_all_comparisons(pdf_path)

        # Para todos los PDFs en un directorio (opcional)
        # pdf_files = Path(pdf_dir).glob('*.pdf')
        # for pdf_file in pdf_files:
        #    await run_all_comparisons(pdf_file)

    asyncio.run(main())
