import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def create_schema_and_table(engine, schema_name, table_name, df):
    """
    Crea un esquema y una tabla en PostgreSQL basada en el DataFrame de Pandas.
    
    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos PostgreSQL.
        schema_name (str): Nombre del esquema.
        table_name (str): Nombre de la tabla.
        df (pd.DataFrame): DataFrame de Pandas con los datos.
    """
    try:
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
            print(f"Esquema '{schema_name}' verificado/creado correctamente.")

            # Crear la tabla si no existe
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {", ".join([f'"{col}" TEXT' for col in df.columns])}
            );
            """
            conn.execute(text(create_table_query))
            print(f"Tabla '{schema_name}.{table_name}' verificada/creada correctamente.")

            conn.commit()

    except Exception as e:
        print(f"Error al crear esquema/tabla: {str(e)}")
        raise

def clean_dataframe(df):
    """
    Limpia el DataFrame eliminando caracteres NUL (\x00) y asegurando que los datos sean cadenas de texto.
    
    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame limpio.
    """
    try:
        # Reemplazar caracteres NUL en columnas de tipo string
        df = df.applymap(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
        print("✔️ Datos limpios de caracteres NUL.")
    except Exception as e:
        print(f"⚠️ Error al limpiar caracteres NUL: {str(e)}")

    return df

def migrate_parquet_to_postgres(parquet_path, schema_name, table_name):
    """
    Migra datos desde un archivo Parquet a PostgreSQL.

    Args:
        parquet_path (str): Ruta al archivo Parquet.
        schema_name (str): Nombre del esquema en PostgreSQL.
        table_name (str): Nombre de la tabla en PostgreSQL.
    """
    try:
        # Leer el archivo Parquet
        print(f"Leyendo archivo Parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)

        # Limpiar caracteres NUL y convertir todo a texto
        df = clean_dataframe(df)

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

        # Crear esquema y tabla si no existen
        create_schema_and_table(engine, schema_name, table_name, df)

        # Migrar los datos
        print(f"Migrando datos a la tabla: {schema_name}.{table_name}")
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='append',  # Cambia a 'replace' si quieres sobrescribir la tabla
            index=False,
            chunksize=1000  # Procesar en lotes para mejor rendimiento
        )

        print(f"✔️ Migración completada exitosamente. {len(df)} filas migradas.")

    except Exception as e:
        print(f"❌ Error durante la migración: {str(e)}")
        raise

def main():
    parquet_path = './outputs/processed_pdf/sections/content_sections_2021_to_2024.parquet'  
    schema_name = 'dncp'  
    table_name = 'pliegos'

    # Ejecutar la migración
    migrate_parquet_to_postgres(parquet_path, schema_name, table_name)


def main():
    
    parquet_path = './outputs/processed_pdf/sections/content_sections_2021_to_2024.parquet'  
    schema_name = 'dncp'  
    table_name = 'pliegos'

    # Ejecutar la migración
    migrate_parquet_to_postgres(parquet_path, schema_name, table_name)

if __name__ == "__main__":
    main()
