import pandas as pd
import requests
from datetime import datetime, timedelta
import os

print("Iniciando captura de datos en la nube...")

# --- 1. DESCARGAR AEMET (DATOS REALES) ---
# Usamos el enlace directo al CSV que no pide contraseña
url_aemet_csv = 'https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_5972X_datos-horarios.csv?k=and&l=5972X&datos=det&w=0&f=temperatura&x='

# Descargamos saltando las 4 primeras líneas de texto de AEMET
try:
    df_reales = pd.read_csv(url_aemet_csv, skiprows=4, encoding='latin1')
    print("✅ Datos reales de AEMET descargados.")
except Exception as e:
    print(f"⚠️ Error AEMET: {e}")
    df_reales = pd.DataFrame() # Tabla vacía por si falla

# --- 2. DESCARGAR OPEN-METEO (PREDICCIONES) ---
url_om = 'https://api.open-meteo.com/v1/forecast?latitude=36.4665&longitude=-6.1962&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FMadrid'
try:
    datos_om = requests.get(url_om).json()
    df_om = pd.DataFrame({
        'Fecha_Hora': datos_om['hourly']['time'],
        'Temp_OM': datos_om['hourly']['temperature_2m'],
        'Lluvia_OM': datos_om['hourly']['precipitation'],
        'Viento_OM': datos_om['hourly']['wind_speed_10m']
    })
    print("✅ Datos de Open-Meteo descargados.")
except Exception as e:
    print(f"⚠️ Error Open-Meteo: {e}")

# --- 3. GUARDAR TODO EN historico.txt ---
# Aquí es donde vosotros y vuestro compañero fusionaríais las tablas usando pandas.
# Por ahora, simplemente guardamos Open-Meteo como ejemplo de que el robot funciona.

nombre_archivo = 'historico.txt'

# Si el archivo ya existe, lo cargamos (para no borrar lo de días anteriores)
if os.path.exists(nombre_archivo):
    df_historico = pd.read_csv(nombre_archivo, sep='\t')
    df_final = pd.concat([df_historico, df_om]).drop_duplicates(subset=['Fecha_Hora'], keep='last')
else:
    df_final = df_om

# Guardamos el archivo actualizado en la carpeta de GitHub
df_final.to_csv(nombre_archivo, sep='\t', index=False)
print("✅ Archivo historico.txt actualizado con éxito. ¡Trabajo terminado!")