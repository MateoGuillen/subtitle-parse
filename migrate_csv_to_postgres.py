import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import logging

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Cargar variables de entorno
load_dotenv()

def migrate_csv_to_unique_titles(csv_path):
    """
    Migra datos desde un archivo CSV a la tabla unique_titles en PostgreSQL.
    
    Args:
        csv_path: Ruta al archivo CSV de entrada
    """
    try:
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
        
        # Verificar/crear esquema y tabla si no existen
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dncp;"))
            logger.info("✔️ Esquema 'dncp' verificado/creado correctamente.")
            
            # Crear tabla `unique_titles` si no existe
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dncp.unique_titles (
                    title TEXT,
                    content TEXT,
                    nro_licitacion TEXT,
                    tipo_variable TEXT,
                    llm_response TEXT
                );
            """))
            logger.info("✔️ Tabla 'dncp.unique_titles' verificada/creada correctamente.")
            conn.commit()

        # Leer el archivo CSV
        logger.info(f"Leyendo archivo CSV: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"✔️ Leídas {len(df)} filas del archivo CSV.")

        # Insertar datos en PostgreSQL
        df.to_sql(
            name='unique_titles',
            con=engine,
            schema='dncp',
            if_exists='append',
            index=False,
            chunksize=1000  # Insertar en lotes de 1000 filas
        )
        
        logger.info(f"✔️ Migración completada: {len(df)} filas insertadas en 'dncp.unique_titles'")

    except Exception as e:
        logger.error(f"❌ Error durante la migración: {str(e)}")
        raise

if __name__ == "__main__":
    # Ruta al archivo CSV
    CSV_PATH = './inputs/unique_titles_whit_clasification.csv'

    # Ejecutar la migración
    migrate_csv_to_unique_titles(CSV_PATH)
