import asyncio
import os
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import pdfplumber
from PyPDF2 import PdfReader
import re
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import platform
import logging
from typing import Optional, Tuple, List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processing.log'),
        logging.StreamHandler()
    ]
)

def configure_system_limits():
    """Configure system-specific limits for better performance"""
    if platform.system() != 'Windows':
        import resource
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

def safe_page_number_extraction(item: Any, reader: PdfReader) -> int:
    """Safely extract page number from outline item"""
    try:
        if isinstance(item.page, int):
            return item.page + 1
        if hasattr(reader, 'get_destination_page_number'):
            return reader.get_destination_page_number(item) + 1
        return 1  # Default to first page if we can't determine
    except Exception as e:
        logging.warning(f"Error extracting page number: {e}")
        return 1

def guardar_lineas_en_txt(lineas: List[str], archivo_salida: str) -> None:
    """
    Guarda las líneas de texto en un archivo de texto.
    
    :param lineas: Lista de líneas de texto extraídas del PDF.
    :param archivo_salida: Ruta del archivo de texto donde se guardarán las líneas.
    """
    try:
        with open(archivo_salida, 'w', encoding='utf-8') as f:
            for linea in lineas:
                f.write(linea + '\n')
        logging.info(f"Líneas guardadas en: {archivo_salida}")
    except Exception as e:
        logging.error(f"Error al guardar las líneas en el archivo de texto: {e}")

class AsyncProcesadorPDF:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self._thread_executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2)
        self.timeout = 60  # Increased timeout to 60 seconds
        self.clean_text_regex = re.compile(r'(\w)\1+|--|::|\.\.|,+|\s+')

    @staticmethod
    def limpiar_texto(texto: str, clean_text_regex) -> str:
        """Clean text with improved handling"""
        try:
            if not isinstance(texto, str):
                texto = str(texto)
            texto = clean_text_regex.sub(lambda x: x.group(1) if x.group(1) else ' ', texto)
            return texto.strip()
        except Exception as e:
            logging.warning(f"Error cleaning text: {e}")
            return ""

    async def safe_pdf_operation(self, operation, *args, **kwargs) -> Any:
        """Execute PDF operations with timeout and error handling"""
        try:
            return await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    self._thread_executor,
                    operation,
                    *args,
                    **kwargs
                ),
                timeout=self.timeout
            )
        except asyncio.TimeoutError:
            logging.error(f"Operation timed out for {self.pdf_path}")
            return None
        except Exception as e:
            logging.error(f"Error in PDF operation: {e}")
            return None

    async def encontrar_linea_titulo(self, titulo: str, pagina: int) -> Tuple[Optional[int], Optional[str]]:
        """
        Find the line number where a title appears in a specific page
        Returns: Tuple of (line_number, full_line_text)
        """
        try:
            # Limpiar el título una sola vez
            clean_titulo = self.limpiar_texto(titulo.lower(), self.clean_text_regex)

            def extract_page_text():
                with pdfplumber.open(self.pdf_path) as pdf:
                    if pagina <= len(pdf.pages):
                        page = pdf.pages[pagina - 1]
                        return page.extract_text()
                return None

            # Ejecutar la extracción de texto de manera asíncrona
            page_text = await asyncio.to_thread(extract_page_text)
            
            if not page_text:
                return None, None

            # Dividir el texto en líneas
            text_lines = page_text.split('\n')
            
            # Buscar el título en cada línea
            for line_num, line in enumerate(text_lines, 1):
                clean_line = self.limpiar_texto(line.lower(), self.clean_text_regex)
                
                # Verificar si el título está en la línea
                if clean_titulo in clean_line:
                    return line_num, line
                    
            return None, None
            
        except Exception as e:
            logging.warning(f"Error finding title line: {e}")
            return None, None

    async def extraer_outline(self) -> List[Dict[str, Any]]:
        """Extract outline with improved error handling"""
        try:
            reader = PdfReader(self.pdf_path, strict=False)
            outline = getattr(reader, 'outline', [])
            if not outline:
                return []

            resultados_outline = []

            async def procesar_subitem(item: Any) -> None:
                try:
                    if hasattr(item, 'title') and hasattr(item, 'page'):
                        page_number = safe_page_number_extraction(item, reader)
                        title = await self.safe_pdf_operation(self.limpiar_texto, item.title, self.clean_text_regex)
                        if title:
                            # Find the line number where the title appears
                            line_number, line_text = await self.encontrar_linea_titulo(title, page_number)
                            
                            # Create the page_line identifier if both values are present
                            page_line = f"{page_number}-{line_number}" if line_number else None
                            
                            resultados_outline.append({
                                "titulo": title,
                                "pagina": page_number,
                                "linea": line_number,
                                "pagina_linea": page_line,
                                "archivo": os.path.basename(self.pdf_path)
                            })
                except Exception as e:
                    logging.warning(f"Error processing outline subitem: {e}")

            async def procesar_item(item: Any) -> None:
                try:
                    if isinstance(item, list):
                        for subitem in item:
                            await procesar_subitem(subitem)
                    else:
                        await procesar_subitem(item)
                except Exception as e:
                    logging.warning(f"Error processing outline item: {e}")

            if isinstance(outline, list):
                for item in outline:
                    await procesar_item(item)
            else:
                await procesar_item(outline)

            return resultados_outline

        except Exception as e:
            logging.error(f"Error extracting outline from {self.pdf_path}: {e}")
            return []

    async def extraer_outline_con_posiciones(self) -> List[Dict[str, Any]]:
        """Extract outline with positions and improved error handling"""
        try:
            outline = await self.extraer_outline()
            if not outline:
                return []

            return outline

        except Exception as e:
            logging.error(f"Error processing positions in {self.pdf_path}: {e}")
            return []

async def procesar_pdf_file(file_path: str) -> List[Dict[str, Any]]:
    """Process a single PDF file with error handling"""
    try:
        procesador = AsyncProcesadorPDF(file_path)
        outlines = await procesador.extraer_outline_con_posiciones()
        
        # Extraer todas las líneas de texto del PDF
        lineas_extraidas = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    lineas_extraidas.extend(page_text.split('\n'))
        
        # Guardar las líneas en un archivo de texto
        archivo_txt = Path("./outputs/processed_pdf/txt") / f"{Path(file_path).stem}_lineas.txt"
        archivo_txt.parent.mkdir(parents=True, exist_ok=True)
        guardar_lineas_en_txt(lineas_extraidas, str(archivo_txt))
        
        return outlines
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return []

async def process_batch(files: List[str], pbar: tqdm) -> List[Dict[str, Any]]:
    """Process a batch of PDF files"""
    tasks = [procesar_pdf_file(file) for file in files]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    processed_results = []
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Batch processing error: {result}")
        else:
            processed_results.extend(result)
    
    pbar.update(len(files))
    return processed_results

async def main():
    output_dir = Path("./outputs/processed_pdf")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Crear el directorio para los archivos de texto
    txt_output_dir = output_dir / "txt"
    txt_output_dir.mkdir(parents=True, exist_ok=True)
    
    base_dir = "./downloads/pdf"
    years = ["2021"]
    
    all_files = []
    for year in years:
        year_dir = Path(base_dir) / year
        if year_dir.exists():
            all_files.extend(list(year_dir.glob("*.pdf")))
    
    if not all_files:
        logging.info("No se encontraron archivos PDF")
        return
    
    logging.info(f"Total de archivos encontrados: {len(all_files)}")
    
    batch_size = 1  # Reduced batch size to 100
    all_outlines = []
    
    with tqdm(total=len(all_files), desc="Procesando archivos PDF") as pbar:
        for i in range(0, len(all_files), batch_size):
            batch = all_files[i:i + batch_size]
            batch_outlines = await process_batch(batch, pbar)
            all_outlines.extend(batch_outlines)
            await asyncio.sleep(0.1)  # Small delay between batches
    
    # Save results
    if all_outlines:
        df_result = pd.DataFrame(all_outlines)
        
        initial_csv_path = output_dir / "outlines_con_fuente.csv"
        df_result.to_csv(initial_csv_path, index=False)
        logging.info(f"\nTotal de outlines encontrados: {len(df_result)}")
        logging.info(f"Resultados guardados en: {initial_csv_path}")
        
        df_unique = df_result.drop_duplicates(subset=['titulo'])
        final_csv_path = output_dir / "outlines_con_fuente_filtered.csv"
        df_unique.to_csv(final_csv_path, index=False)
        logging.info(f"Resultados filtrados guardados en: {final_csv_path}")
    else:
        logging.warning("No se encontraron outlines para procesar")

if __name__ == "__main__":
    configure_system_limits()
    
    loop = asyncio.new_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4))
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()