import asyncio
import aiohttp
import aiofiles
import zipfile
import rarfile
import os
import pandas as pd


class FileHandler:
    def __init__(self, file_path="./resources/licitaciones_con_pliego.csv", 
                 output_dir="./downloads", low_memory=False, 
                 batch_size=1000, pause_time=300):
        self.file_path = file_path
        self.output_dir = output_dir
        self.low_memory = low_memory
        self.batch_size = batch_size
        self.pause_time = pause_time
        self.current_position = 0
        self.data = None
        self._load_data()
        os.makedirs(self.output_dir, exist_ok=True)

    def _load_data(self):
        """
        Carga los datos desde un archivo CSV especificado en file_path.
        """
        try:
            self.data = pd.read_csv(self.file_path, low_memory=self.low_memory)
            # Convert categoria_id to int, handling NaN values
            self.data['categoria_id'] = self.data['categoria_id'].fillna(-1).astype(int)
            print(f"Archivo '{self.file_path}' cargado con éxito.")
        except Exception as e:
            raise ValueError(f"No se pudo cargar el archivo CSV: {e}")

    async def _extract_and_rename_pdf(self, compressed_file, nro_licitacion, categoria_id, date):
        """
        Extrae un archivo PDF desde un archivo comprimido (.zip o .rar) y lo renombra, luego elimina el archivo comprimido.
        """
        try:
            if zipfile.is_zipfile(compressed_file):
                await self._extract_and_rename(compressed_file, 'zip', nro_licitacion, categoria_id, date)
            elif rarfile.is_rarfile(compressed_file):
                await self._extract_and_rename(compressed_file, 'rar', nro_licitacion, categoria_id, date)
            else:
                print(f"El archivo comprimido no es válido: {compressed_file}")
                return

            os.remove(compressed_file)
            print(f"Archivo comprimido eliminado: {compressed_file}")

        except Exception as e:
            print(f"Error al extraer o renombrar el archivo PDF: {e}")

    async def _extract_and_rename(self, compressed_file, file_type, nro_licitacion, categoria_id, date):
        """
        Extrae y renombra el archivo PDF desde un archivo comprimido (.zip o .rar).
        """
        try:
            file_handlers = {
                'zip': zipfile.ZipFile,
                'rar': rarfile.RarFile,
            }

            if file_type not in file_handlers:
                print(f"Tipo de archivo no soportado: {file_type}")
                return

            # Crear el manejador de archivo de forma síncrona
            handler = file_handlers[file_type](compressed_file, 'r')
            
            try:
                # Crear un directorio temporal único para esta extracción
                temp_dir = os.path.join(self.output_dir, f"temp_{date}_{categoria_id}_{nro_licitacion}")
                os.makedirs(temp_dir, exist_ok=True)
                
                # Buscar y extraer el archivo PDF
                for file_name in handler.namelist():
                    if file_name.endswith("01-pliego-de-bases-y-condiciones-pbc.pdf"):
                        # Extraer primero al directorio temporal
                        extracted_path = await asyncio.to_thread(handler.extract, file_name, temp_dir)
                        
                        # Construir la ruta final del archivo
                        final_pdf_name = f"{date}_{categoria_id}_{nro_licitacion}.pdf"
                        final_path = os.path.join(self.output_dir, final_pdf_name)
                        
                        # Mover el archivo a su ubicación final
                        os.replace(extracted_path, final_path)
                        print(f"Archivo PDF extraído y renombrado a: {final_path}")
                        
                        # Eliminar el directorio temporal
                        await asyncio.to_thread(self._remove_temp_dir, temp_dir)
                        break
            finally:
                # Asegurarse de cerrar el archivo
                handler.close()
                    
        except Exception as e:
            print(f"Error al extraer del archivo {file_type.upper()}: {e}")
            # Intentar limpiar el directorio temporal en caso de error
            if 'temp_dir' in locals():
                await asyncio.to_thread(self._remove_temp_dir, temp_dir)

    def _remove_temp_dir(self, temp_dir):
        """
        Elimina el directorio temporal y su contenido de forma segura.
        """
        try:
            if os.path.exists(temp_dir):
                for root, dirs, files in os.walk(temp_dir, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(temp_dir)
        except Exception as e:
            print(f"Error al eliminar directorio temporal {temp_dir}: {e}")

    async def _rename_and_log(self, extracted_path, nro_licitacion, categoria_id, date):
        """
        Renombra el archivo extraído y registra el cambio.
        """
        new_file_name = f"{date}_{categoria_id}_{nro_licitacion}.pdf"
        new_path = os.path.join(self.output_dir, new_file_name)
        os.rename(extracted_path, new_path)
        print(f"Archivo PDF extraído y renombrado a: {new_path}")

    async def _get_file_extension_from_headers(self, url):
        """
        Obtiene la extensión del archivo desde los encabezados HTTP.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(url, allow_redirects=True) as response:
                    content_disposition = response.headers.get('Content-Disposition', '')
                    content_type = response.headers.get('Content-Type', '')
                    default_ext = ".zip"

                    if "filename=" in content_disposition:
                        file_name = content_disposition.split("filename=")[-1].strip('"')
                        _, ext = os.path.splitext(file_name)
                        return ext

                    if "zip" in content_type:
                        return default_ext
                    elif "rar" in content_type:
                        return ".rar"
                    else:
                        return default_ext
        except Exception as e:
            print(f"Error al obtener la extensión desde los encabezados: {e}")
            return default_ext

    async def download_files_from_urls(self):
        """
        Descarga los archivos PDF desde las URLs proporcionadas en el archivo CSV.
        """
        if self.data is None or 'tender_documents_url' not in self.data.columns:
            raise ValueError("El archivo CSV debe contener una columna 'tender_documents_url'.")

        async with aiohttp.ClientSession() as session:
            tasks = []
            for _, row in self.data.iterrows():
                if pd.isna(row['tender_documents_url']):
                    continue  # Skip rows with NaN URLs
                    
                url = row['tender_documents_url']
                nro_licitacion = row['nro_licitacion']
                categoria_id = row['categoria_id']  # Now safely converted to int
                date = str(row['date'])[:4]

                task = asyncio.create_task(
                    self._download_and_process_file(session, url, nro_licitacion, categoria_id, date)
                )
                tasks.append(task)

            # Wait for all tasks to complete
            downloaded_files = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None values and exceptions
            return [f for f in downloaded_files if f is not None and not isinstance(f, Exception)]



    async def _download_and_process_file(self, session, url, nro_licitacion, categoria_id, date):
        """
        Descarga y procesa un archivo desde una URL con reintentos y manejo de excepciones.
        """
        retries = 6  # Número de intentos en caso de error
        for attempt in range(retries):
            try:
                # Realizar la solicitud HTTP
                async with session.get(url) as response:
                    response.raise_for_status()  # Lanza una excepción si el estado HTTP es 4xx/5xx

                    # Obtener la extensión del archivo
                    ext = await self._get_file_extension_from_headers(url)
                    if not ext:  # Si no se puede obtener la extensión, se usa .zip por defecto
                        print(f"No se pudo obtener la extensión para {url}, se utilizará la predeterminada '.zip'")
                        ext = ".zip"

                    file_name = f"{date}_{categoria_id}_{nro_licitacion}{ext}"
                    file_name_without_ext = f"{date}_{categoria_id}_{nro_licitacion}"
                    output_path = os.path.join(self.output_dir, file_name)

                    # Descargar el archivo en bloques
                    async with aiofiles.open(output_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(1024):
                            await file.write(chunk)

                    print(f"Archivo descargado: {output_path}")

                    # Verificar si el archivo existe antes de intentar extraerlo
                    if not os.path.exists(output_path):
                        print(f"El archivo descargado no existe en la ruta {output_path}")
                        return None

                    # Intentar extraer y renombrar el archivo
                    await self._extract_and_rename_pdf(output_path, nro_licitacion, categoria_id, date)
                    return file_name_without_ext

            except aiohttp.ClientError as e:
                print(f"Error de conexión o solicitud HTTP con '{url}': {e}")
            except Exception as e:
                print(f"Error inesperado al descargar desde '{url}': {e}")

            # Esperar antes de reintentar
            if attempt < retries - 1:
                print(f"Reintentando... (Intento {attempt + 1} de {retries})")
                await asyncio.sleep(6)  # Espera antes de reintentar

        # Si fallan todos los intentos
        print(f"Se agotaron los intentos para descargar el archivo desde '{url}'")
        return None
    async def download_files_from_urls_batch(self):
        """
        Downloads files in batches from URLs provided in the CSV file.
        Returns the list of downloaded files in the current batch.
        """
        if self.data is None or 'tender_documents_url' not in self.data.columns:
            raise ValueError("CSV file must contain a 'tender_documents_url' column.")

        # Get the next batch of records
        start_idx = self.current_position
        end_idx = min(start_idx + self.batch_size, len(self.data))
        
        # If we've processed all records, return None
        if start_idx >= len(self.data):
            return None
            
        batch_data = self.data.iloc[start_idx:end_idx]
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for _, row in batch_data.iterrows():
                if pd.isna(row['tender_documents_url']):
                    continue
                    
                url = row['tender_documents_url']
                nro_licitacion = row['nro_licitacion']
                categoria_id = row['categoria_id']
                date = str(row['date'])[:4]

                task = asyncio.create_task(
                    self._download_and_process_file(session, url, nro_licitacion, categoria_id, date)
                )
                tasks.append(task)

            # Wait for all tasks in the batch to complete
            downloaded_files = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update the position for the next batch
            self.current_position = end_idx
            
            # Filter out None values and exceptions
            return [f for f in downloaded_files if f is not None and not isinstance(f, Exception)]
    async def _download_json_file(self, session, url, nro_licitacion, categoria_id, date):
        """
        Downloads a JSON file directly from a URL with retries and exception handling.
        Returns the filename if successful, None otherwise.
        """
        retries = 6
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()

                    # Create filename with .json extension
                    file_name = f"{date}_{categoria_id}_{nro_licitacion}.json"
                    output_path = os.path.join(self.output_dir, file_name)

                    # Download and save the file
                    async with aiofiles.open(output_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(1024):
                            await file.write(chunk)

                    print(f"JSON file downloaded: {output_path}")
                    return file_name.replace('.json', '')  # Return filename without extension

            except aiohttp.ClientError as e:
                print(f"Connection or HTTP request error with '{url}': {e}")
            except Exception as e:
                print(f"Unexpected error downloading from '{url}': {e}")

            # Wait before retrying
            if attempt < retries - 1:
                print(f"Retrying... (Attempt {attempt + 1} of {retries})")
                await asyncio.sleep(6)

        print(f"All attempts exhausted for downloading file from '{url}'")
        return None

    async def download_json_files_batch(self):
        """
        Downloads JSON files in batches from URLs provided in the CSV file.
        Returns the list of downloaded files in the current batch.
        """
        if self.data is None or 'tender_documents_url' not in self.data.columns:
            raise ValueError("CSV file must contain a 'tender_documents_url' column.")

        # Get the next batch of records
        start_idx = self.current_position
        end_idx = min(start_idx + self.batch_size, len(self.data))
        
        # If we've processed all records, return None
        if start_idx >= len(self.data):
            return None
            
        batch_data = self.data.iloc[start_idx:end_idx]
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for _, row in batch_data.iterrows():
                if pd.isna(row['tender_documents_url']):
                    continue
                    
                url = row['tender_documents_url']
                nro_licitacion = row['nro_licitacion']
                categoria_id = row['categoria_id']
                date = str(row['date'])[:4]

                task = asyncio.create_task(
                    self._download_json_file(session, url, nro_licitacion, categoria_id, date)
                )
                tasks.append(task)

            # Wait for all tasks in the batch to complete
            downloaded_files = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update the position for the next batch
            self.current_position = end_idx
            
            # Filter out None values and exceptions
            return [f for f in downloaded_files if f is not None and not isinstance(f, Exception)]    
