import pandas as pd
import requests
import io
import os

print("Iniciando captura de datos integrada...")

# --- 1. DESCARGAR OPEN-METEO (PREDICCIONES) ---
# Añadimos &forecast_days=2 para tener exactamente 48 horas (hoy y mañana)
url_om = 'https://api.open-meteo.com/v1/forecast?latitude=36.4665&longitude=-6.1962&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FMadrid&forecast_days=2'
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
    df_om = pd.DataFrame(columns=['Fecha_Hora', 'Temp_OM', 'Lluvia_OM', 'Viento_OM'])

# --- 2. DESCARGAR AEMET (DATOS REALES) ---
url_aemet_csv = 'https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_5972X_datos-horarios.csv?k=and&l=5972X&datos=det&w=0&f=temperatura&x='
try:
    # TRUCO: Nos disfrazamos de navegador web para que AEMET no nos bloquee
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    respuesta = requests.get(url_aemet_csv, headers=headers)
    respuesta.encoding = 'latin1'
    
    # Leemos el CSV desde el texto de la respuesta
    df_reales = pd.read_csv(io.StringIO(respuesta.text), skiprows=4)
    
    # Limpiamos los nombres de las columnas por si AEMET ha metido espacios raros
    df_reales.columns = df_reales.columns.str.strip()
    
    # Formateamos la fecha para que sea idéntica a la de Open-Meteo
    df_reales['Fecha_Hora'] = pd.to_datetime(df_reales['Fecha y hora oficial'], format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%dT%H:%M')
    
    # Seleccionamos y renombramos
    df_reales = df_reales[['Fecha_Hora', 'Temperatura (ºC)', 'Velocidad del viento (km/h)', 'Precipitación (mm)']]
    df_reales.rename(columns={
        'Temperatura (ºC)': 'Temp_AEMET', 
        'Velocidad del viento (km/h)': 'Viento_AEMET', 
        'Precipitación (mm)': 'Lluvia_AEMET'
    }, inplace=True)
    print("✅ Datos reales de AEMET descargados y formateados.")
except Exception as e:
    print(f"⚠️ Error AEMET: {e}")
    df_reales = pd.DataFrame(columns=['Fecha_Hora', 'Temp_AEMET', 'Viento_AEMET', 'Lluvia_AEMET'])

# --- 3. FUSIONAR Y GUARDAR ---
df_hoy = pd.merge(df_om, df_reales, on='Fecha_Hora', how='left')

nombre_archivo = 'historico.txt'
if os.path.exists(nombre_archivo):
    df_historico = pd.read_csv(nombre_archivo, sep='\t')
    df_final = pd.concat([df_historico, df_hoy]).drop_duplicates(subset=['Fecha_Hora'], keep='last')
else:
    df_final = df_hoy

df_final.to_csv(nombre_archivo, sep='\t', index=False)
print("✅ Archivo historico.txt actualizado con éxito.")
