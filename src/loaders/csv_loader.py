""" Utilities for loading and saving CSV files. """ 
import os
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling

class CSVLoader:
    """A class for loading and saving CSV files."""
    def __init__(self, output_dir, output_processed_dir):
        self.output_dir = output_dir
        self.output_processed_dir = output_processed_dir
        self.logger = setup_logger(__name__)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.output_processed_dir, exist_ok=True)

    def save_csv(self, df, filename, output_dir):
        """Guarda un DataFrame como archivo CSV."""
        output_path = os.path.join(output_dir, filename)
        try:
            df.to_csv(output_path, index=False)
            self.logger.info("Datos guardados en %s", output_path)
            return output_path
        except Exception as e:
            self.logger.error("Error al guardar CSV: %s", e)
            raise
    @error_handling(default_return=(None, None))
    def save_yearly_data(self, pdf_df, json_df, year, prefix_name):
        """
        Guarda los dataframes de un año específico.
        
        Args:
            pdf_df: DataFrame con datos de PDFs
            json_df: DataFrame con datos de JSONs
            year: Año de los datos
            prefix_name: Prefijo para los nombres de archivos
            
        Returns:
            tuple: (ruta_pdf, ruta_json) con las rutas donde se guardaron los archivos
        """
        pdf_path = None
        json_path = None

        if pdf_df is not None:
            pdf_filename = f"{prefix_name}_pdf_{year}.csv"
            pdf_path = self.save_csv(pdf_df, pdf_filename, self.output_processed_dir)
            self.logger.info("Datos PDF del año %s guardados correctamente", year)

        if json_df is not None:
            json_filename = f"{prefix_name}_json_{year}.csv"
            json_path = self.save_csv(json_df, json_filename, self.output_processed_dir)
            self.logger.info("Datos JSON del año %s guardados correctamente", year)

        return pdf_path, json_path
    @error_handling(default_return=(None, None))
    def save_merged_data(self, pdf_df, json_df, pdf_filename, json_filename):
        """
        Guarda los dataframes combinados de múltiples años.
        
        Args:
            pdf_df: DataFrame combinado con datos de PDFs
            json_df: DataFrame combinado con datos de JSONs
            pdf_filename: Nombre del archivo para guardar datos PDF
            json_filename: Nombre del archivo para guardar datos JSON
            
        Returns:
            tuple: (ruta_pdf, ruta_json) con las rutas donde se guardaron los archivos
        """
        pdf_path = None
        json_path = None

        if pdf_df is not None:
            pdf_path = self.save_csv(pdf_df, pdf_filename, self.output_processed_dir)
            self.logger.info("Datos PDF combinados guardados en %s", pdf_filename)

        if json_df is not None:
            json_path = self.save_csv(json_df, json_filename, self.output_processed_dir)
            self.logger.info("Datos JSON combinados guardados en %s", json_filename)

        return pdf_path, json_path
