from pydataxm import *                          
import datetime as dt  

import psycopg2
import os
from dotenv import load_dotenv

#objetoAPI = pydataxm.ReadDB()
#df = objetoAPI.get_collections('DispoDeclarada')
#print(df.to_dict())

def etl_dd(date_from:dt.date=None, date_to:dt.date=None):

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
        'DispoDeclarada',
        'Recurso',
        date_from,
        date_to)

    for row in df.index:
        fecha = df['Date'].loc[row]
        values_code = df['Values_code'].loc[row]
        values_hour01 = df['Values_Hour01'].loc[row]
        values_hour02 = df['Values_Hour02'].loc[row]
        values_hour03 = df['Values_Hour03'].loc[row]
        values_hour04 = df['Values_Hour04'].loc[row]
        values_hour05 = df['Values_Hour05'].loc[row]
        values_hour06 = df['Values_Hour06'].loc[row]
        values_hour07 = df['Values_Hour07'].loc[row]
        values_hour08 = df['Values_Hour08'].loc[row]
        values_hour09 = df['Values_Hour09'].loc[row]
        values_hour10 = df['Values_Hour10'].loc[row]
        values_hour11 = df['Values_Hour11'].loc[row]
        values_hour12 = df['Values_Hour12'].loc[row]
        values_hour13 = df['Values_Hour13'].loc[row]
        values_hour14 = df['Values_Hour14'].loc[row]
        values_hour15 = df['Values_Hour15'].loc[row]
        values_hour16 = df['Values_Hour16'].loc[row]
        values_hour17 = df['Values_Hour17'].loc[row]
        values_hour18 = df['Values_Hour18'].loc[row]
        values_hour19 = df['Values_Hour19'].loc[row]
        values_hour20 = df['Values_Hour20'].loc[row]
        values_hour21 = df['Values_Hour21'].loc[row]
        values_hour22 = df['Values_Hour22'].loc[row]
        values_hour23 = df['Values_Hour23'].loc[row]
        values_hour24 = df['Values_Hour24'].loc[row]

        sql = '''INSERT INTO disponibilidad_declarada
            (fecha,
                values_code, 
                values_hour01,
                values_hour02,
                values_hour03,
                values_hour04,
                values_hour05,
                values_hour06,
                values_hour07,
                values_hour08,
                values_hour09,
                values_hour10,
                values_hour11,
                values_hour12,
                values_hour13,
                values_hour14,
                values_hour15,
                values_hour16,
                values_hour17,
                values_hour18,
                values_hour19,
                values_hour20,
                values_hour21,
                values_hour22,
                values_hour23,
                values_hour24) 
                VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s);'''

        val =  (fecha,
                values_code, 
                values_hour01,
                values_hour02,
                values_hour03,
                values_hour04,
                values_hour05,
                values_hour06,
                values_hour07,
                values_hour08,
                values_hour09,
                values_hour10,
                values_hour11,
                values_hour12,
                values_hour13,
                values_hour14,
                values_hour15,
                values_hour16,
                values_hour17,
                values_hour18,
                values_hour19,
                values_hour20,
                values_hour21,
                values_hour22,
                values_hour23,
                values_hour24) 
                
        try:  # identificamos si ya existe en la bd
            cur.execute(f"SELECT * FROM disponibilidad_declarada WHERE fecha='{fecha}' AND values_code='{values_code}';")
            r = len(cur.fetchall())
            if r == 0:
                cur.execute(sql, val)
                conn.commit()
        except: # continua si ya existe en la bd
            continue

    conn.close()


etl_dd(date_from=dt.date(2022,1,1), date_to=dt.date(2022,12,31))