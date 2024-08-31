from pydataxm import *                          
import datetime as dt  
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np
from psycopg2 import extras  # Asegúrate de importar el submódulo extras

# Carga las variables de entorno al inicio del script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path)

def get_db_connection():
    """Establece una conexión con la base de datos PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DBNAME'), 
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD')
    )

def convert_numpy_to_python(value):
    """Convierte valores de numpy a tipos de datos nativos de Python."""
    if isinstance(value, np.generic):
        return value.item()  # Convierte valores numpy a tipos nativos de Python
    return value

def etl_dd(date_from: dt.date = None, date_to: dt.date = None):
    """
    Ejecuta un proceso ETL para extraer datos de la API 'DispoDeclarada' y almacenarlos en una base de datos PostgreSQL.
    
    Parameters:
        date_from (dt.date): Fecha de inicio para la extracción de datos.
        date_to (dt.date): Fecha de fin para la extracción de datos.
    """
    # Validación de fechas predeterminadas
    if date_from is None or date_to is None:
        date_from = dt.date(2022, 12, 30)
        date_to = dt.date(2022, 12, 31)

    # Extracción de datos usando la API
    objetoAPI = pydataxm.ReadDB()
    df = objetoAPI.request_data(
        'DispoDeclarada',
        'Recurso',
        date_from,
        date_to
    )

    # Estructura para insertar datos
    records_to_insert = []
    for _, row in df.iterrows():
        record = tuple(convert_numpy_to_python(row[col]) for col in df.columns)
        records_to_insert.append(record[1:])

    # Inserción masiva de datos en la base de datos
    insert_query = '''
        INSERT INTO xm.disponibilidad_declarada (
            values_code, values_hour01, values_hour02, values_hour03,
            values_hour04, values_hour05, values_hour06, values_hour07,
            values_hour08, values_hour09, values_hour10, values_hour11,
            values_hour12, values_hour13, values_hour14, values_hour15,
            values_hour16, values_hour17, values_hour18, values_hour19,
            values_hour20, values_hour21, values_hour22, values_hour23,
            values_hour24, fecha
        ) VALUES %s
        ON CONFLICT (fecha, values_code) DO NOTHING;
    '''

    # Conexión y ejecución en la base de datos
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_values(  # Uso correcto de extras.execute_values
                    cur, insert_query, records_to_insert, template=None
                )
            conn.commit()
    except Exception as e:
        print(f"Error during database operation: {e}")

# Ejecución del proceso ETL con las fechas proporcionadas
etl_dd(date_from=dt.date(2024, 8, 1), date_to=dt.date(2024, 8, 30))
