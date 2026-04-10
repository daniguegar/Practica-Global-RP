import pandas as pd
import requests
import os

print("Iniciando captura de datos integrada...")

# --- 1. DESCARGAR OPEN-METEO (PREDICCIONES) ---
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
    df_om = pd.DataFrame(columns=['Fecha_Hora', 'Temp_OM', 'Lluvia_OM', 'Viento_OM'])

# --- 2. DESCARGAR AEMET (DATOS REALES) ---
url_aemet_csv = 'https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_5972X_datos-horarios.csv?k=and&l=5972X&datos=det&w=0&f=temperatura&x='
try:
    # AEMET tiene 4 líneas de cabecera antes de la tabla real
    df_reales = pd.read_csv(url_aemet_csv, skiprows=4, encoding='latin1')
    
    # Traducir la fecha de AEMET (DD/MM/YYYY HH:MM) al formato de Open-Meteo (YYYY-MM-DDTHH:MM)
    df_reales['Fecha_Hora'] = pd.to_datetime(df_reales['Fecha y hora oficial'], format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%dT%H:%M')
    
    # Quedarnos solo con las columnas que nos importan y renombrarlas
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

# --- 3. FUSIONAR LAS TABLAS ---
# Unimos las dos tablas basándonos en la columna 'Fecha_Hora'. 
# how='left' significa que mantiene todas las horas de Open-Meteo y añade los datos de AEMET donde coincidan.
df_hoy = pd.merge(df_om, df_reales, on='Fecha_Hora', how='left')

# --- 4. ACTUALIZAR EL HISTÓRICO ---
nombre_archivo = 'historico.txt'

if os.path.exists(nombre_archivo):
    # Cargamos el histórico antiguo
    df_historico = pd.read_csv(nombre_archivo, sep='\t')
    # Añadimos los datos de hoy, y si hay horas repetidas, nos quedamos con las más recientes ('last')
    df_final = pd.concat([df_historico, df_hoy]).drop_duplicates(subset=['Fecha_Hora'], keep='last')
else:
    df_final = df_hoy

# Guardar el archivo separando las columnas por tabulaciones
df_final.to_csv(nombre_archivo, sep='\t', index=False)
print("✅ Archivo historico.txt actualizado con la fusión de AEMET y Open-Meteo.")
