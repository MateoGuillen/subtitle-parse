import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import numpy as np
import re
import logging
import io

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Cargar variables de entorno
load_dotenv()

def create_schema_and_tables(engine):
    """
    Crea el esquema y las tablas en PostgreSQL si no existen.
    """
    try:
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dncp;"))
            logger.info("✔️ Esquema 'dncp' verificado/creado correctamente.")

            # Crear tabla `categorias`
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.categorias (
                    category_id TEXT PRIMARY KEY,
                    descripcion TEXT
                );
            """))
            logger.info("✔️ Tabla 'dncp.categorias' verificada/creada correctamente.")

            # Crear tabla `licitaciones`
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.licitaciones (
                    nro_licitacion TEXT PRIMARY KEY,
                    category_id TEXT REFERENCES dncp.categorias (category_id),
                    year TEXT
                );
            """))
            logger.info("✔️ Tabla 'dncp.licitaciones' verificada/creada correctamente.")

            # Crear tabla `pliegos` particionada
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.pliegos (
                    document_id TEXT,
                    nro_licitacion TEXT REFERENCES dncp.licitaciones (nro_licitacion),
                    title TEXT,
                    content TEXT,
                    page TEXT,
                    line_start TEXT,
                    line_end TEXT,
                    depth TEXT,
                    content_length TEXT,
                    year TEXT
                ) PARTITION BY LIST (year);
            """))
            logger.info("✔️ Tabla 'dncp.pliegos' verificada/creada correctamente.")

            # Crear particiones para `pliegos`
            for year in ['2021', '2022', '2023', '2024']:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS dncp.pliegos_{year} PARTITION OF dncp.pliegos
                    FOR VALUES IN ('{year}');
                """))
                logger.info(f"✔️ Partición 'dncp.pliegos_{year}' verificada/creada correctamente.")

            conn.commit()

    except Exception as e:
        logger.error(f"❌ Error al crear esquema/tablas: {str(e)}")
        raise

def deep_clean_text(text):
    """
    Limpieza profunda de texto para eliminar caracteres problemáticos.
    
    Args:
        text: Texto a limpiar
        
    Returns:
        str: Texto limpio
    """
    if not isinstance(text, str):
        return text
    
    try:
        # Eliminar caracteres NUL (0x00)
        cleaned = text.replace('\x00', '')
        
        # Eliminar todos los caracteres de control (excepto espacios en blanco comunes)
        cleaned = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F-\x9F]', '', cleaned)
        
        # Normalizar caracteres Unicode (descomponer caracteres con acentos y eliminar caracteres no imprimibles)
        import unicodedata
        cleaned = unicodedata.normalize('NFKD', cleaned)
        
        # Reemplazar caracteres no ASCII con sus equivalentes o eliminarlos si no hay equivalente
        cleaned = ''.join(c if ord(c) < 128 else ' ' for c in cleaned)
        
        # Eliminar múltiples espacios en blanco consecutivos
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        return cleaned.strip()
    except Exception as e:
        logger.warning(f"Error durante la limpieza de texto: {str(e)} - Retornando cadena vacía")
        return ""

def clean_dataframe(df):
    """
    Limpia todos los campos de texto en un DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a limpiar
        
    Returns:
        pandas.DataFrame: DataFrame limpio
    """
    df_cleaned = df.copy()
    
    # Lista de columnas de texto que necesitan limpieza
    text_columns = ['document_id', 'nro_licitacion', 'title', 'content',
                   'page', 'line_start', 'line_end', 'depth', 'content_length', 'year']
    
    # Limpiar solo las columnas de texto que existen en el DataFrame
    for col in [c for c in text_columns if c in df_cleaned.columns]:
        if pd.api.types.is_object_dtype(df_cleaned[col]):
            logger.info(f"Limpiando columna '{col}'...")
            df_cleaned[col] = df_cleaned[col].apply(deep_clean_text)
    
    return df_cleaned

def export_problem_rows(problem_rows, filename="problematic_rows.csv"):
    """
    Exporta las filas problemáticas a un archivo CSV para análisis posterior.
    
    Args:
        problem_rows (list): Lista de filas problemáticas (DataFrames de una fila)
        filename (str): Nombre del archivo de salida
    """
    if problem_rows:
        try:
            combined_df = pd.concat(problem_rows, ignore_index=True)
            combined_df.to_csv(filename, index=False)
            logger.info(f"✔️ {len(problem_rows)} filas problemáticas exportadas a '{filename}'")
        except Exception as e:
            logger.error(f"❌ Error al exportar filas problemáticas: {str(e)}")

def migrate_parquet_to_postgres(engine, parquet_path):
    """
    Migra datos desde un archivo Parquet a PostgreSQL con limpieza exhaustiva.
    """
    try:
        # Leer el archivo Parquet
        logger.info(f"Leyendo archivo Parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        logger.info(f"✔️ Leídas {len(df)} filas del archivo Parquet.")

        # Renombrar `cleaned_content` a `content` si existe
        if 'cleaned_content' in df.columns:
            df.rename(columns={'cleaned_content': 'content'}, inplace=True)
            logger.info("✔️ Columna 'cleaned_content' renombrada a 'content'.")

        # Reemplazar valores NaN con None
        df = df.replace({np.nan: None})
        logger.info("✔️ Valores NaN reemplazados con None.")
        
        # Realizar limpieza exhaustiva del DataFrame
        df = clean_dataframe(df)
        logger.info("✔️ DataFrame completamente limpiado.")

        # Lista para almacenar filas problemáticas
        problem_rows = []
        
        # Migrar datos por lotes para cada año
        logger.info("Poblando tabla 'dncp.pliegos'...")
        for year in ['2021', '2022', '2023', '2024']:
            # Filtrar datos por año
            year_df = df[df['year'] == year]
            
            if not year_df.empty:
                # Seleccionar solo las columnas necesarias
                columns_to_select = [col for col in [
                    'document_id', 'nro_licitacion', 'title', 'content',
                    'page', 'line_start', 'line_end', 'depth', 'content_length', 'year'
                ] if col in year_df.columns]
                
                pliegos_df = year_df[columns_to_select]
                
                # Eliminar columnas duplicadas (si las hay)
                pliegos_df = pliegos_df.loc[:, ~pliegos_df.columns.duplicated()]

                # Definir tamaño del lote
                batch_size = 1000  # Lotes más pequeños para mejor manejo
                total_rows = len(pliegos_df)
                num_batches = (total_rows + batch_size - 1) // batch_size
                
                logger.info(f"Insertando {total_rows} filas en 'dncp.pliegos_{year}' en {num_batches} lotes...")
                
                # Para hacer seguimiento del progreso
                total_inserted = 0
                total_errors = 0
                
                # Procesar por lotes
                for i in range(num_batches):
                    start_idx = i * batch_size
                    end_idx = min((i + 1) * batch_size, total_rows)
                    batch_df = pliegos_df.iloc[start_idx:end_idx]
                    
                    try:
                        # Convertir a CSV y luego leerlo de nuevo para asegurar una limpieza adicional
                        csv_buffer = io.StringIO()
                        batch_df.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)
                        clean_batch = pd.read_csv(csv_buffer)
                        
                        # Insertar en PostgreSQL
                        clean_batch.to_sql(
                            name=f'pliegos_{year}',
                            con=engine,
                            schema='dncp',
                            if_exists='append',
                            index=False
                        )
                        total_inserted += len(batch_df)
                        logger.info(f"  ✔️ Lote {i+1}/{num_batches}: {len(batch_df)} filas insertadas.")
                    except Exception as batch_error:
                        logger.error(f"  ❌ Error en lote {i+1}/{num_batches}: {str(batch_error)}")
                        
                        # Intentar inserción fila por fila
                        logger.info(f"    Intentando inserción fila por fila para el lote problemático...")
                        success_count = 0
                        
                        for idx, row in batch_df.iterrows():
                            try:
                                # Limpiar la fila específica
                                row_df = pd.DataFrame([row])
                                clean_row = clean_dataframe(row_df)
                                
                                # Intentar insertar después de limpieza adicional
                                clean_row.to_sql(
                                    name=f'pliegos_{year}',
                                    con=engine,
                                    schema='dncp',
                                    if_exists='append',
                                    index=False
                                )
                                success_count += 1
                            except Exception as row_error:
                                logger.warning(f"    ❌ Error en fila {idx}: {str(row_error)}")
                                total_errors += 1
                                # Guardar fila problemática para análisis posterior
                                problem_rows.append(pd.DataFrame([row]))
                        
                        total_inserted += success_count
                        logger.info(f"    ✔️ {success_count}/{len(batch_df)} filas insertadas individualmente.")
                
                logger.info(f"✔️ Año {year}: {total_inserted} filas insertadas, {total_errors} filas con error.")
            else:
                logger.warning(f"⚠️ No hay datos para el año {year}.")
        
        # Exportar filas problemáticas para análisis posterior
        export_problem_rows(problem_rows)
        
        logger.info("✔️ Migración completada.")

    except Exception as e:
        logger.error(f"❌ Error durante la migración: {str(e)}")
        raise

def main():
    # Rutas a los archivos
    csv_path = './inputs/unique_categories_sorted.csv'
    parquet_path = './outputs/processed_pdf/sections/todos_2021_to_2024/content_sections_cleaned_2021_to_2024.parquet'

    # Obtener credenciales desde variables de entorno
    db_params = {
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    # Crear la cadena de conexión
    connection_string = (
        f"postgresql://{db_params['user']}:{db_params['password']}"
        f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    )

    # Crear el motor de SQLAlchemy
    logger.info("Creando conexión a la base de datos...")
    engine = create_engine(connection_string)

    # Crear esquema y tablas si no existen
    create_schema_and_tables(engine)

    # Migrar datos desde el archivo Parquet
    migrate_parquet_to_postgres(engine, parquet_path)

if __name__ == "__main__":
    main()