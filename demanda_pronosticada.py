import requests
import pandas as pd

req = requests.get('https://app-portalxmcore01.azurewebsites.net/administracion-archivos/ficheros/mostrar-url?ruta=M:/InformacionAgentes/Usuarios/Publico/DEMANDAS/Pronostico Oficial/2022-12/PRONSIN1226.txt')
link = str(req.content)[10:-3]

#link = str(req.content)
print(link)

file = 'demanda.csv'

print("")
resp = requests.get(link)
output = open(file, 'wb')
output.write(b'\n')
output.write(resp.content)
output.close()
print("Archivo descargado:",file)


df = pd.read_csv('demanda.csv')
print(df)