import asyncio
import os
from utils.file_async_json_processor import JSONExtractor
from tqdm import tqdm

async def process_json_file(json_path, semaphore, output_dir):
    """Procesa un archivo JSON con un límite de concurrencia."""
    async with semaphore:  # Limita la concurrencia
        try:
            extractor = JSONExtractor(json_path)
            await extractor.load_json()
            clauses = extractor.extract_clauses()
            if clauses:  # Verificar que hay cláusulas antes de guardar
                await extractor.save_to_csv(clauses, output_dir)
            else:
                print(f"No se encontraron cláusulas en {json_path}")
        except Exception as e:
            print(f"Error procesando {json_path}: {e}")

async def process_batch(batch, semaphore, pbar, output_dir):
    """Procesa un lote de archivos JSON."""
    tasks = []
    for json_file in batch:
        task = asyncio.create_task(process_json_file(json_file, semaphore, output_dir))
        tasks.append(task)
    
    for task in asyncio.as_completed(tasks):
        try:
            await task
        except Exception as e:
            print(f"Error en la tarea: {e}")
        finally:
            pbar.update(1)

async def main():
    year = "2021"
    json_dir = f"./downloads/json/{year}"
    output_dir = f"./outputs/processed_json/{year}"
    
    # Verificar que el directorio existe
    if not os.path.exists(json_dir):
        raise FileNotFoundError(f"El directorio {json_dir} no existe")
    
    json_files = [os.path.join(json_dir, f) for f in os.listdir(json_dir) if f.endswith('.json')]
    
    if not json_files:
        print(f"No se encontraron archivos JSON en {json_dir}")
        return

    # Configuración de semáforo y lotes
    concurrency_limit = 50  # Número máximo de tareas concurrentes
    batch_size = 500  # Número de archivos por lote
    semaphore = asyncio.Semaphore(concurrency_limit)

    # Barra de progreso
    with tqdm(total=len(json_files), desc="Procesando archivos") as pbar:
        # Procesar por lotes
        for i in range(0, len(json_files), batch_size):
            batch = json_files[i:i + batch_size]
            print(f"\nProcesando lote {i // batch_size + 1} de {len(json_files) // batch_size + 1}")
            await process_batch(batch, semaphore, pbar, output_dir)
            print(f"Lote {i // batch_size + 1} completado")

if __name__ == "__main__":
    asyncio.run(main())