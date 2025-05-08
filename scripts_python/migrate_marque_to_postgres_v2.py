import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import numpy as np
import re

# Cargar variables de entorno
load_dotenv()

def create_schema_and_tables(engine):
    """
    Crea el esquema y las tablas en PostgreSQL si no existen.
    
    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos PostgreSQL.
    """
    try:
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dncp;"))
            print("✔️ Esquema 'dncp' verificado/creado correctamente.")

            # Crear tabla `categorias`
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.categorias (
                    category_id TEXT PRIMARY KEY,
                    descripcion TEXT
                );
            """))
            print("✔️ Tabla 'dncp.categorias' verificada/creada correctamente.")

            # Crear tabla `licitaciones`
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.licitaciones (
                    nro_licitacion TEXT PRIMARY KEY,
                    category_id TEXT REFERENCES dncp.categorias (category_id),
                    year TEXT
                );
            """))
            print("✔️ Tabla 'dncp.licitaciones' verificada/creada correctamente.")

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
            print("✔️ Tabla 'dncp.pliegos' verificada/creada correctamente.")

            # Crear particiones para `pliegos`
            for year in ['2021', '2022', '2023', '2024']:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS dncp.pliegos_{year} PARTITION OF dncp.pliegos
                    FOR VALUES IN ('{year}');
                """))
                print(f"✔️ Partición 'dncp.pliegos_{year}' verificada/creada correctamente.")

            conn.commit()

    except Exception as e:
        print(f"❌ Error al crear esquema/tablas: {str(e)}")
        raise

def populate_categorias(engine, csv_path):
    """
    Pobla la tabla `dncp.categorias` desde un archivo CSV.

    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos PostgreSQL.
        csv_path (str): Ruta al archivo CSV de categorías.
    """
    try:
        # Leer el archivo CSV
        print(f"Leyendo archivo CSV: {csv_path}")
        categorias_df = pd.read_csv(csv_path, sep=',')  

        # Renombrar columnas para que coincidan con la tabla
        categorias_df.rename(columns={
            'category_id': 'category_id',
            'name': 'descripcion'
        }, inplace=True)

        # Limpiar caracteres especiales
        for col in categorias_df.columns:
            categorias_df[col] = categorias_df[col].fillna('').astype(str)
            categorias_df[col] = categorias_df[col].apply(lambda x: re.sub(r'[\x00-\x1F\x7F]', '', x))

        # Poblar la tabla `categorias`
        print("Poblando tabla 'dncp.categorias'...")
        categorias_df.to_sql(
            name='categorias',
            con=engine,
            schema='dncp',
            if_exists='append',
            index=False
        )
        print(f"✔️ {len(categorias_df)} filas insertadas en 'dncp.categorias'.")

    except Exception as e:
        print(f"❌ Error al poblar 'dncp.categorias': {str(e)}")
        raise

def clean_text_for_postgres(text):
    """
    Limpia una cadena de texto para que sea compatible con PostgreSQL.
    Elimina caracteres nulos y otros caracteres de control problemáticos,
    pero mantiene los saltos de línea y retornos de carro.
    
    Args:
        text: Texto a limpiar
        
    Returns:
        str: Texto limpio
    """
    if pd.isna(text) or text is None:
        return ''
    
    # Convertir a string si no lo es
    text = str(text)
    
    # Eliminar solo caracteres nulos y otros caracteres problemáticos específicos
    # Mantener \n (10), \r (13), y \t (9)
    cleaned_text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    
    # Asegurar que los saltos de línea estén preservados correctamente
    # Normalizar diferentes tipos de saltos de línea
    cleaned_text = cleaned_text.replace('\r\n', '\n').replace('\r', '\n')
    
    return cleaned_text
def migrate_parquet_to_postgres(engine, parquet_path):
    """
    Migra datos desde un archivo Parquet a PostgreSQL.

    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos PostgreSQL.
        parquet_path (str): Ruta al archivo Parquet.
    """
    try:
        # Leer el archivo Parquet
        print(f"Leyendo archivo Parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"DataFrame cargado. Dimensiones: {df.shape}")

        # Asegurarse de que 'cleaned_content' se copie a 'content'
        if 'cleaned_content' in df.columns:
            if 'content' in df.columns:
                # Si ambas columnas existen, sobrescribe 'content' con 'cleaned_content'
                df['content'] = df['cleaned_content']
                # Elimina la columna 'cleaned_content' para evitar duplicados
                df = df.drop(columns=['cleaned_content'])
            else:
                # Si solo existe 'cleaned_content', renómbrala a 'content'
                df.rename(columns={'cleaned_content': 'content'}, inplace=True)
            print("✔️ Datos de 'cleaned_content' transferidos a 'content' correctamente.")
            
        # Reemplazar valores NaN con strings vacíos
        df = df.fillna('')
        print("✔️ Valores NaN reemplazados con cadenas vacías.")

        # Limpiar todos los campos de texto para eliminar caracteres nulos
        text_columns = df.select_dtypes(include=['object']).columns.tolist()
        print(f"Limpiando caracteres nulos de columnas: {text_columns}")

        for col in text_columns:
            print(f"Procesando columna: {col}")
            # Utilizar la función clean_text_for_postgres
            df[col] = df[col].apply(clean_text_for_postgres)

        print("✔️ Todos los caracteres nulos eliminados correctamente.")

        # Poblar la tabla `pliegos` usando to_sql para cada año
        print("Poblando tabla 'dncp.pliegos'...")
        for year in ['2021', '2022', '2023', '2024']:
            # Filtrar datos por año
            year_data = df[df['year'] == year]
            
            if not year_data.empty:
                # Seleccionar solo las columnas necesarias y evitar duplicados
                pliegos_df = year_data[['document_id', 'nro_licitacion', 'title', 'content',
                                        'page', 'line_start', 'line_end', 'depth', 'content_length', 'year']]
                
                # Eliminar columnas duplicadas (si las hay)
                pliegos_df = pliegos_df.loc[:, ~pliegos_df.columns.duplicated()]
                
                print(f"Insertando {len(pliegos_df)} filas para el año {year}...")
                
                # Insertar en lotes para mejor manejo de memoria
                batch_size = 10000
                total_rows = len(pliegos_df)
                
                for i in range(0, total_rows, batch_size):
                    end_idx = min(i + batch_size, total_rows)
                    batch = pliegos_df.iloc[i:end_idx].copy()
                    
                    # Usar la función clean_text_for_postgres una última vez
                    for col in batch.columns:
                        batch[col] = batch[col].apply(clean_text_for_postgres)
                    
                    batch.to_sql(
                        name=f'pliegos_{year}',
                        con=engine,
                        schema='dncp',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    print(f"  ✔️ Lote {i//batch_size + 1}: {end_idx - i} filas insertadas.")
                
                print(f"✔️ Total: {total_rows} filas insertadas en 'dncp.pliegos_{year}'.")
            else:
                print(f"⚠️ No hay datos para el año {year}.")

        print("✔️ Migración completada exitosamente.")

    except Exception as e:
        print(f"❌ Error durante la migración: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise
def populate_licitaciones(engine, parquet_path):
    """
    Pobla la tabla `dncp.licitaciones` con datos extraídos del archivo Parquet.

    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos PostgreSQL.
        parquet_path (str): Ruta al archivo Parquet que contiene los datos de licitaciones.
    """
    try:
        # Leer el archivo Parquet
        print(f"Leyendo archivo Parquet para licitaciones: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"DataFrame cargado. Dimensiones: {df.shape}")

        # Extraer los datos únicos de licitaciones (nro_licitacion, year)
        # Si category_id está en el DataFrame, incluirlo; si no, usar NULL
        columns_to_select = ['nro_licitacion', 'year']
        if 'category_id' in df.columns:
            columns_to_select.append('category_id')
        
        # Obtener licitaciones únicas
        licitaciones_df = df[columns_to_select].drop_duplicates(subset=['nro_licitacion'])
        
        # Asegurarse de que no hay valores nulos en nro_licitacion
        licitaciones_df = licitaciones_df[licitaciones_df['nro_licitacion'].notna()]
        
        # Limpiar los datos
        for col in licitaciones_df.columns:
            licitaciones_df[col] = licitaciones_df[col].apply(clean_text_for_postgres)
        
        # Si no existe category_id en el DataFrame, añadirla como NULL
        if 'category_id' not in licitaciones_df.columns:
            licitaciones_df['category_id'] = None
        
        print(f"Se han encontrado {len(licitaciones_df)} licitaciones únicas.")
        
        # Insertar datos en la tabla licitaciones
        print("Insertando datos en la tabla 'dncp.licitaciones'...")
        
        # Usar inserción por lotes para mejor manejo de memoria
        batch_size = 1000
        total_rows = len(licitaciones_df)
        
        for i in range(0, total_rows, batch_size):
            end_idx = min(i + batch_size, total_rows)
            batch = licitaciones_df.iloc[i:end_idx].copy()
            
            try:
                batch.to_sql(
                    name='licitaciones',
                    con=engine,
                    schema='dncp',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                print(f"  ✔️ Lote {i//batch_size + 1}: {end_idx - i} licitaciones insertadas.")
            except Exception as e:
                print(f"  ⚠️ Error al insertar lote {i//batch_size + 1}: {str(e)}")
                # Si hay error en este lote, intentar insertar fila por fila para identificar el problema
                for j in range(i, end_idx):
                    try:
                        single_row = licitaciones_df.iloc[[j]]
                        single_row.to_sql(
                            name='licitaciones',
                            con=engine,
                            schema='dncp',
                            if_exists='append',
                            index=False
                        )
                    except Exception as row_error:
                        print(f"    ❌ Error al insertar fila {j}: {str(row_error)}")
                        print(f"    Datos de la fila: {licitaciones_df.iloc[j].to_dict()}")
        
        print(f"✔️ Proceso completado. Se insertaron datos en 'dncp.licitaciones'.")
        
    except Exception as e:
        print(f"❌ Error durante la población de licitaciones: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise
def main():
    # Rutas a los archivos
    csv_path = './inputs/unique_categories_sorted.csv'  # Ruta al archivo CSV de categorías
    parquet_path = './outputs/processed_pdf/sections/todos_2021_to_2024/content_sections_cleaned_2021_to_2024.parquet'  # Ruta al archivo Parquet limpio

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
    engine = create_engine(connection_string)

    # Crear esquema y tablas si no existen
    create_schema_and_tables(engine)

    # Poblar la tabla `categorias` desde el archivo CSV
    # populate_categorias(engine, csv_path)
    # populate_licitaciones(engine, parquet_path)

    # Migrar datos desde el archivo Parquet
    migrate_parquet_to_postgres(engine, parquet_path)

if __name__ == "__main__":
    main()