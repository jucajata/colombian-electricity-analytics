from pydataxm import *                          
import datetime as dt  
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np
from psycopg2 import extras
from flask import Flask, request

# Cargar variables de entorno al inicio del script
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
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
        return value.item()
    return value

def etl_dd(date_from: dt.date = None, date_to: dt.date = None):
    """Proceso ETL para extraer datos y actualizarlos en la base de datos PostgreSQL."""
    
    # Usar valores predeterminados si no se proporcionan
    if date_to is None:
        date_to = dt.date.today()
    if date_from is None:
        date_from = date_to - dt.timedelta(days=30)

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
        records_to_insert.append(record[1:])  # Excluye el primer dato (FECHA)

    # Inserción o actualización de datos en la base de datos
    update_query = '''
        INSERT INTO xm.disponibilidad_declarada (
            values_code, values_hour01, values_hour02, values_hour03,
            values_hour04, values_hour05, values_hour06, values_hour07,
            values_hour08, values_hour09, values_hour10, values_hour11,
            values_hour12, values_hour13, values_hour14, values_hour15,
            values_hour16, values_hour17, values_hour18, values_hour19,
            values_hour20, values_hour21, values_hour22, values_hour23,
            values_hour24, fecha
        ) VALUES %s
        ON CONFLICT (fecha, values_code) 
        DO UPDATE SET
            values_hour01 = EXCLUDED.values_hour01,
            values_hour02 = EXCLUDED.values_hour02,
            values_hour03 = EXCLUDED.values_hour03,
            values_hour04 = EXCLUDED.values_hour04,
            values_hour05 = EXCLUDED.values_hour05,
            values_hour06 = EXCLUDED.values_hour06,
            values_hour07 = EXCLUDED.values_hour07,
            values_hour08 = EXCLUDED.values_hour08,
            values_hour09 = EXCLUDED.values_hour09,
            values_hour10 = EXCLUDED.values_hour10,
            values_hour11 = EXCLUDED.values_hour11,
            values_hour12 = EXCLUDED.values_hour12,
            values_hour13 = EXCLUDED.values_hour13,
            values_hour14 = EXCLUDED.values_hour14,
            values_hour15 = EXCLUDED.values_hour15,
            values_hour16 = EXCLUDED.values_hour16,
            values_hour17 = EXCLUDED.values_hour17,
            values_hour18 = EXCLUDED.values_hour18,
            values_hour19 = EXCLUDED.values_hour19,
            values_hour20 = EXCLUDED.values_hour20,
            values_hour21 = EXCLUDED.values_hour21,
            values_hour22 = EXCLUDED.values_hour22,
            values_hour23 = EXCLUDED.values_hour23,
            values_hour24 = EXCLUDED.values_hour24;
    '''

    # Conexión y ejecución en la base de datos
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                extras.execute_values(
                    cur, update_query, records_to_insert
                )
            conn.commit()
    except Exception as e:
        print(f"Error during database operation: {e}")

def main(request):
    """Función principal que maneja la solicitud HTTP."""
    
    # Obtener parámetros de la URL
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Convertir las fechas de la URL si existen
    if date_from:
        date_from = dt.datetime.strptime(date_from, '%Y-%m-%d').date()
    if date_to:
        date_to = dt.datetime.strptime(date_to, '%Y-%m-%d').date()

    # Ejecutar la función ETL con los parámetros proporcionados o con los valores predeterminados
    etl_dd(date_from=date_from, date_to=date_to)
    
    return "ETL process completed successfully", 200