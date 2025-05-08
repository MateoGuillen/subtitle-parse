import asyncio
import logging
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

async def merge_parquet_files(output_dir: Path, batch_size: int = 100000, year: str = "1111"):
    """Optimized Parquet file merging"""
    try:
        parquet_files = list(output_dir.glob("*.parquet"))
        if len(parquet_files) <= 1:
            logging.info("No hay suficientes archivos Parquet para fusionar.")
            return

        # Leer tablas en paralelo
        async def read_table(file):
            return await asyncio.to_thread(pq.read_table, str(file))

        tasks = [read_table(file) for file in parquet_files]
        tables = await asyncio.gather(*tasks)

        # Concatenar y escribir el archivo final
        combined_table = pa.concat_tables(tables)
        final_path = output_dir / f'combined_documents_{year}.parquet'
        
        await asyncio.to_thread(
            pq.write_table,
            combined_table,
            final_path,
            compression='snappy',
            row_group_size=batch_size
        )

        # Eliminar archivos intermedios en paralelo
        await asyncio.gather(*[
            asyncio.to_thread(file.unlink)
            for file in parquet_files
            if file != final_path
        ])

        logging.info(f"Todos los archivos se han fusionado en: {final_path}")

    except Exception as e:
        logging.error(f"Error al fusionar archivos Parquet: {e}")

def main():
    # Configura el logging
    logging.basicConfig(level=logging.INFO)

    # Especifica la carpeta donde están los archivos Parquet
    output_dir = Path("./outputs/processed_pdf/todos_anhios")
    year = "2021_to_2025"

    # Ejecuta la función asíncrona
    asyncio.run(merge_parquet_files(output_dir, year=year))

if __name__ == "__main__":
    main()