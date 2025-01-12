import pandas as pd
import requests
import os
import zipfile
import rarfile  # Necesitarás instalarlo con `pip install rarfile`
from urllib.parse import urlparse
from urllib.request import urlopen

class LicitacionesDownloader:
    def __init__(self, file_path="./resources/licitaciones_con_pliego.csv", output_dir="./downloads"):
        """Constructor de la clase LicitacionesDownloader."""
        self.file_path = file_path
        self.output_dir = output_dir
        self.data = None
        self._load_data()

        # Crear el directorio de descargas si no existe
        os.makedirs(self.output_dir, exist_ok=True)

    def _load_data(self):
        """Cargar los datos del archivo CSV."""
        try:
            self.data = pd.read_csv(self.file_path)
            print(f"Archivo '{self.file_path}' cargado con éxito.")
        except Exception as e:
            raise ValueError(f"No se pudo cargar el archivo CSV: {e}")

    def filter_csv(self, output_file="./outputs/filtered_licitaciones.csv", categoria=None, nro_licitacion=None, fecha=None, cantidad=2):
        """
        Filtrar el archivo CSV y guardar el resultado en un nuevo archivo CSV.
        """
        if self.data is None:
            raise ValueError("No se han cargado datos del archivo CSV.")

        filtered_data = self.data

        if categoria is not None:
            filtered_data = filtered_data[filtered_data['categoria_id'] == categoria]

        if nro_licitacion is not None:
            filtered_data = filtered_data[filtered_data['nro_licitacion'] == nro_licitacion]

        if fecha is not None:
            filtered_data = filtered_data[filtered_data['fecha_publicacion_convocatoria'] == fecha]

        filtered_data = filtered_data.head(cantidad)

        if filtered_data.empty:
            raise ValueError("No se encontraron registros con los filtros especificados.")

        filtered_data.to_csv(output_file, index=False)
        print(f"Archivo filtrado guardado como '{output_file}'.")
        return output_file



    def _extract_and_rename_pdf(self, compressed_file, nro_licitacion, categoria_id, fecha):
        """
        Extrae un archivo PDF desde un archivo comprimido (.zip o .rar) y lo renombra, luego elimina el archivo comprimido.

        Args:
            compressed_file (str): Ruta del archivo comprimido.
            nro_licitacion (str): Número de la licitación.
            categoria_id (int): ID de la categoría.
            fecha (str): Fecha de la convocatoria.
        """
        try:
            # Verificar si es .zip o .rar y llamar a la función de extracción y renombrado correspondiente
            if zipfile.is_zipfile(compressed_file):
                self._extract_and_rename(compressed_file, 'zip', nro_licitacion, categoria_id, fecha)
            elif rarfile.is_rarfile(compressed_file):
                self._extract_and_rename(compressed_file, 'rar', nro_licitacion, categoria_id, fecha)
            else:
                print(f"El archivo comprimido no es válido: {compressed_file}")
                return

            # Eliminar el archivo comprimido una vez extraído el PDF
            os.remove(compressed_file)
            print(f"Archivo comprimido eliminado: {compressed_file}")

        except Exception as e:
            print(f"Error al extraer o renombrar el archivo PDF: {e}")

    def _extract_and_rename(self, compressed_file, file_type, nro_licitacion, categoria_id, fecha):
        """
        Extrae y renombra el archivo PDF desde un archivo comprimido (.zip o .rar).
        """
        try:
            # Diccionario que mapea el tipo de archivo a la clase correspondiente
            file_handlers = {
                'zip': zipfile.ZipFile,
                'rar': rarfile.RarFile
            }

            # Verificar si el tipo de archivo es soportado
            if file_type not in file_handlers:
                print(f"Tipo de archivo no soportado: {file_type}")
                return

            # Usar la clase correspondiente para abrir el archivo comprimido
            with file_handlers[file_type](compressed_file, 'r') as archive:
                for file_name in archive.namelist():
                    if file_name.endswith("01-pliego-de-bases-y-condiciones-pbc.pdf"):
                        extracted_path = archive.extract(file_name, self.output_dir)
                        self._rename_and_log(extracted_path, nro_licitacion, categoria_id, fecha)
                        break
        except Exception as e:
            print(f"Error al extraer del archivo {file_type.upper()}: {e}")

    def _rename_and_log(self, extracted_path, nro_licitacion, categoria_id, fecha):
        """
        Renombra el archivo extraído y registra el cambio.
        """
        new_file_name = f"{nro_licitacion}_{categoria_id}_{fecha}.pdf"
        new_path = os.path.join(self.output_dir, new_file_name)
        os.rename(extracted_path, new_path)
        print(f"Archivo PDF extraído y renombrado a: {new_path}")


    def _get_file_extension_from_headers(self, url):
        """
        Obtiene la extensión del archivo desde los encabezados HTTP.
        """
        try:
            # Realizar una solicitud HEAD para obtener los encabezados sin descargar el archivo
            response = requests.head(url, allow_redirects=True)
            content_disposition = response.headers.get('Content-Disposition', '')
            content_type = response.headers.get('Content-Type', '')
            default_ext = ".zip"

            # Verificar si Content-Disposition contiene un nombre de archivo
            if "filename=" in content_disposition:
                file_name = content_disposition.split("filename=")[-1].strip('"')
                _, ext = os.path.splitext(file_name)
                return ext

            # Verificar el tipo de contenido en Content-Type
            if "zip" in content_type:
                return default_ext
            elif "rar" in content_type:
                return ".rar"
            else:
                return default_ext  # Asume default_ext si no se puede determinar
        except Exception as e:
            print(f"Error al obtener la extensión desde los encabezados: {e}")
            return default_ext  # Asume default_ext en caso de error

    def download_files_from_urls(self, input_file):
        """
        Descarga los archivos PDF desde las URLs proporcionadas en el archivo CSV.
        """
        try:
            data = pd.read_csv(input_file)
        except Exception as e:
            raise ValueError(f"No se pudo cargar el archivo CSV: {e}")

        if 'url' not in data.columns:
            raise ValueError("El archivo CSV debe contener una columna 'url'.")

        for idx, row in data.iterrows():
            url = row['url']
            nro_licitacion = row['nro_licitacion']
            categoria_id = row['categoria_id']
            fecha_publicacion_convocatoria = row['fecha_publicacion_convocatoria']

            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()

                # Determinar la extensión del archivo desde los encabezados HTTP
                ext = self._get_file_extension_from_headers(url)
                file_name = f"{nro_licitacion}_{categoria_id}_{fecha_publicacion_convocatoria}{ext}"
                output_path = os.path.join(self.output_dir, file_name)

                # Descargar el archivo
                with open(output_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)

                print(f"Archivo descargado: {output_path}")

                # Extraer y renombrar el archivo PDF
                self._extract_and_rename_pdf(output_path, nro_licitacion, categoria_id, fecha_publicacion_convocatoria)
            except Exception as e:
                print(f"Error al descargar desde URL '{url}': {e}")

# Uso de la clase
if __name__ == "__main__":
    downloader = LicitacionesDownloader()
    filtered_csv = downloader.filter_csv(categoria=1, cantidad=5)
    downloader.download_files_from_urls(filtered_csv)
