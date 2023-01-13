import requests
import pandas as pd
import os
from datetime import datetime, date, timedelta
import psycopg2
from dotenv import load_dotenv

def etl_podd(year:int=None):

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('./Dev')))
    dotenv_path = os.path.join(BASE_DIR, 'colombian-electricity-analytics/.env')
    load_dotenv(dotenv_path)

    # Connect to your postgres DB
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DBNAME'), 
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD'))

    # Open a cursor to perform database operations
    cur = conn.cursor()

    date_now = datetime.now()

    if year is None:
        year = date_now.year

    # obtención de días lunes

    year_obj = year
    fecha = date(year,1,1)
    list_mondays = []  # lista vacía que tendrá objetos datetime

    while True:
        if year != year_obj:
            break  # condicional para que no sobrepase el último día del año que se quiere cargar
        elif fecha > date(date_now.year, date_now.month, date_now.day) + timedelta(days=7):
            break  # condicional para que la búsqueda no sobrepase el día actual + 7 días
        else:
            if fecha.weekday() == 0:  # condicional para identificar los lunes
                list_mondays.append(fecha)  # agregamos la fecha a la lista de lunes
            fecha = fecha + timedelta(days=1)  # sumamos un día a la fecha
            year = fecha.year  # actualizamos la variable year


    # ETL

    columnas_df = [
        'Values_Hour01', 'Values_Hour02', 'Values_Hour03', 'Values_Hour04',
        'Values_Hour05', 'Values_Hour06', 'Values_Hour07', 'Values_Hour08',
        'Values_Hour09', 'Values_Hour10', 'Values_Hour11', 'Values_Hour12',
        'Values_Hour13', 'Values_Hour14', 'Values_Hour15', 'Values_Hour16',
        'Values_Hour17', 'Values_Hour18', 'Values_Hour19', 'Values_Hour20',
        'Values_Hour21', 'Values_Hour22', 'Values_Hour23', 'Values_Hour24'
    ]

    df_total = pd.DataFrame(columns=columnas_df)
    for monday in list_mondays:
        anio = str(monday.year)
        mes = str(monday.month)
        if len(mes) == 1:
            mes = '0' + mes
        dia = str(monday.day)
        if len(dia) == 1:
            dia = '0' + dia

        anio_mes = anio+'-'+mes
        mes_dia = mes+dia
        url_file = f'/{anio_mes}/PRONSIN{mes_dia}.txt'

        # obtención del link para poder descargar la información desde la página de XM de la demanda pronosticada
        url_init = 'https://app-portalxmcore01.azurewebsites.net/administracion-archivos/ficheros/mostrar-url'
        url_end = f'?ruta=M:/InformacionAgentes/Usuarios/Publico/DEMANDAS/Pronostico Oficial{url_file}'

        url = url_init + url_end
        resp = requests.get(url)
        link = str(resp.content)[10:-3]

        # descarga del archivo objetivo
        file = 'demanda_pronosticada.csv'
        resp = requests.get(link)
        output = open(file, 'wb')
        output.write(resp.content)
        output.close()

        # lectura y eliminación del csv descargado
        df = pd.read_csv('demanda_pronosticada.csv')
        os.remove('demanda_pronosticada.csv')

        # transformación de las columnas y filas del df
        fila1 = list(df.columns)  # obtenemos los valores de las columnas
        df.loc[len(df)] = fila1  # agregamos esa fila al df

        list_dates = [monday]
        for i in range(1, 7):
            fecha_actualizar = monday + timedelta(days=i)
            list_dates.append(fecha_actualizar)        

        df.columns = ['Hora'] + list_dates
        df.index = df['Hora'].astype(int)
        df = df.drop(columns=['Hora'])
        
        for column in df.columns:
            df[column] = pd.to_numeric(df[column])
        df = df.sort_index()
        df = df.T
        df.columns = columnas_df
        df_total = pd.concat([df_total, df])

    df_total['Values_code'] = 'Sistema'
    df = df_total
    df['Date'] = df.index

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

        sql = '''INSERT INTO pronostico_oficial_demanda_definitivo 
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
            cur.execute(f"SELECT * FROM pronostico_oficial_demanda_definitivo  WHERE fecha='{fecha}';")
            r = len(cur.fetchall())
            if r == 0:
                cur.execute(sql, val)
                conn.commit()
        except: # continua si ya existe en la bd
            continue

    conn.close()

etl_podd(year=2022)