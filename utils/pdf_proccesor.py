from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.config.parser import ConfigParser
from marker.output import text_from_rendered

class PDFProcessor:
    def __init__(self, config):
        """
        Inicializa el procesador de PDFs con la configuración dada.
        """
        self.config_parser = ConfigParser(config)
        self.converter = PdfConverter(
            config=self.config_parser.generate_config_dict(),
            artifact_dict=create_model_dict(),
            processor_list=self.config_parser.get_processors(),
            renderer=self.config_parser.get_renderer()
        )

    def process_pdf(self, file_path, output_file):
        """
        Procesa el archivo PDF y guarda el contenido HTML en un archivo.

        Args:
            file_path (str): Ruta del archivo PDF a procesar.
            output_file (str): Nombre del archivo de salida para guardar el HTML.
        """
        rendered = self.converter(file_path)

        html_output = rendered.html
        metadata = rendered.metadata
        images = rendered.images

        # Guardar el contenido HTML en un archivo
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(html_output)

        return metadata, images
