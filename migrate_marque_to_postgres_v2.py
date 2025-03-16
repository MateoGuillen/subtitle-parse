import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

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
        categorias_df = pd.read_csv(csv_path, sep='\t')  # Usar '\t' como separador

        # Renombrar columnas para que coincidan con la tabla
        categorias_df.rename(columns={
            'compiledRelease/parties/0/details/categories/0/id': 'category_id',
            'compiledRelease/parties/0/details/categories/0/name': 'descripcion'
        }, inplace=True)

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

        # Renombrar `cleaned_content` a `content`
        if 'cleaned_content' in df.columns:
            df.rename(columns={'cleaned_content': 'content'}, inplace=True)
            print("✔️ Columna 'cleaned_content' renombrada a 'content'.")

        # Poblar la tabla `licitaciones`
        print("Poblando tabla 'dncp.licitaciones'...")
        licitaciones_df = df[['nro_licitacion', 'category_id', 'year']].drop_duplicates()
        licitaciones_df.to_sql(
            name='licitaciones',
            con=engine,
            schema='dncp',
            if_exists='append',
            index=False
        )
        print(f"✔️ {len(licitaciones_df)} filas insertadas en 'dncp.licitaciones'.")

        # Poblar la tabla `pliegos`
        print("Poblando tabla 'dncp.pliegos'...")
        pliegos_df = df[[
            'document_id', 'nro_licitacion', 'title', 'content',
            'page', 'line_start', 'line_end', 'depth', 'content_length', 'year'
        ]]
        pliegos_df.to_sql(
            name='pliegos',
            con=engine,
            schema='dncp',
            if_exists='append',
            index=False,
            chunksize=1000  # Procesar en lotes para mejor rendimiento
        )
        print(f"✔️ {len(pliegos_df)} filas insertadas en 'dncp.pliegos'.")

        print("✔️ Migración completada exitosamente.")

    except Exception as e:
        print(f"❌ Error durante la migración: {str(e)}")
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
    populate_categorias(engine, csv_path)

    # Migrar datos desde el archivo Parquet
    migrate_parquet_to_postgres(engine, parquet_path)

if __name__ == "__main__":
    main()