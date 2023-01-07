from pydataxm import *                          
import datetime as dt  

import psycopg2
import os
from dotenv import load_dotenv

#df = objetoAPI.get_collections('ListadoAgentes')
#print(df.to_dict())

def etl_laa(date_from:dt.date=None, date_to:dt.date=None):

    if date_from is None or date_to is None:
        date_from = dt.date(2022,12,30)
        date_to = dt.date(2022,12,31)

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('./Dev')))
    dotenv_path = os.path.join(BASE_DIR, 'colombian-electricity-analytics/.env')
    load_dotenv(dotenv_path)

    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DBNAME'), 
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'))

    # Open a cursor to perform database operations
    cur = conn.cursor()

    objetoAPI = pydataxm.ReadDB()

    df = objetoAPI.request_data(
        'ListadoAgentes',
        'Sistema',
        date_from,
        date_to)

    for row in df.index:
        fecha = df['Date'].loc[row]
        values_code = df['Values_Code'].loc[row]
        values_name = df['Values_Name'].loc[row]
        values_activity = df['Values_Activity'].loc[row]
        values_operstartdate = df['Values_OperStartdate'].loc[row]
        values_state = df['Values_State'].loc[row]

        sql = '''INSERT INTO listado_agentes_atributos 
               (fecha,
                values_code, 
                values_name,
                values_activity,
                values_operstartdate,
                values_state) 
                VALUES (
                %s, %s, %s, %s, %s, %s);'''

        val =  (fecha,
                values_code, 
                values_name,
                values_activity,
                values_operstartdate,
                values_state) 
                
        try:  # identificamos si ya existe en la bd
            cur.execute(f"SELECT * FROM listado_agentes_atributos WHERE fecha='{fecha}' AND values_code='{values_code}';")
            r = len(cur.fetchall())
            if r == 0:
                cur.execute(sql, val)
                conn.commit()
        except: # continua si ya existe en la bd
            continue

    conn.close()


etl_laa(date_from=dt.date(2023,1,1), date_to=dt.date(2023,1,7))