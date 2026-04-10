import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import io
import os

print("Iniciando proceso de actualización del histórico (Versión MATLAB port)...")

# --- CONFIGURACIÓN Y TIEMPOS ---
archivo_txt = 'historico.txt'
tz = pytz.timezone('Europe/Madrid')
hora_actual = datetime.now(tz)
hora_base = hora_actual.replace(minute=0, second=0, microsecond=0)

# Generamos array con las próximas 48 horas exactas
fechas_objetivo_dt = [hora_base + timedelta(hours=i) for i in range(1, 49)]
fechas_str = [dt.strftime('%Y-%m-%d %H:00') for dt in fechas_objetivo_dt]

# DataFrame temporal para las 48h de predicción nuevas
df_nuevas = pd.DataFrame({'Fecha_Objetivo': fechas_str})
df_nuevas['Fecha_Captura'] = hora_actual.strftime('%Y-%m-%d %H:%M')

# --- FASE 1: OPEN-METEO (Predicciones) ---
print("Fase 1: Descargando Open-Meteo...")
url_om = 'https://api.open-meteo.com/v1/forecast?latitude=36.4665&longitude=-6.1962&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FMadrid&forecast_days=3'
try:
    datos_om = requests.get(url_om).json()
    df_om = pd.DataFrame({
        'Fecha_Objetivo': pd.to_datetime(datos_om['hourly']['time']).strftime('%Y-%m-%d %H:%M'),
        'Temp_OM': datos_om['hourly']['temperature_2m'],
        'Lluvia_OM': datos_om['hourly']['precipitation'],
        'Viento_OM': datos_om['hourly']['wind_speed_10m']
    })
    df_nuevas = pd.merge(df_nuevas, df_om, on='Fecha_Objetivo', how='left')
except Exception as e:
    print(f"⚠️ Error OM: {e}")
    df_nuevas[['Temp_OM', 'Lluvia_OM', 'Viento_OM']] = None

# --- FASE 2: AEMET PREDICCIONES (XML) ---
print("Fase 2: Descargando AEMET Predicciones (XML)...")
url_aemet_pred = 'https://www.aemet.es/xml/municipios_h/localidad_h_11031.xml'
temp_aemet, lluvia_aemet, viento_aemet = {}, {}, {}

def dato_seguro(valor):
    if not valor:
        return None
    # Quitamos espacios y cambiamos comas por puntos (por si acaso)
    valor = valor.strip().replace(',', '.')
    # Si AEMET manda "Ip" (Inapreciable), lo forzamos a 0.0 para que Python no se congele
    if valor.lower() == 'ip':
        return 0.0
    try:
        return float(valor)
    except ValueError:
        return None

try:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    respuesta_xml = requests.get(url_aemet_pred, headers=headers)
    root = ET.fromstring(respuesta_xml.content)
    
    for dia in root.findall('.//dia'):
        fecha_dia = dia.get('fecha')
        if not fecha_dia: continue
        
        for temp in dia.findall('.//temperatura'):
            periodo = temp.get('periodo')
            if periodo and temp.text:
                hora_str = f"{fecha_dia} {periodo.zfill(2)}:00"
                temp_aemet[hora_str] = dato_seguro(temp.text)
                
        for prec in dia.findall('.//precipitacion'):
            periodo = prec.get('periodo')
            if periodo and prec.text:
                hora_str = f"{fecha_dia} {periodo.zfill(2)}:00"
                lluvia_aemet[hora_str] = dato_seguro(prec.text)
                
        for viento in dia.findall('.//viento'):
            periodo = viento.get('periodo')
            vel = viento.find('velocidad')
            if periodo and vel is not None and vel.text:
                hora_str = f"{fecha_dia} {periodo.zfill(2)}:00"
                viento_aemet[hora_str] = dato_seguro(vel.text)

    df_nuevas['Temp_AEMET'] = df_nuevas['Fecha_Objetivo'].map(temp_aemet)
    df_nuevas['Lluvia_AEMET'] = df_nuevas['Fecha_Objetivo'].map(lluvia_aemet)
    df_nuevas['Viento_AEMET'] = df_nuevas['Fecha_Objetivo'].map(viento_aemet)

except Exception as e:
    print(f"⚠️ Error AEMET XML: {e}")

# --- FASE 3: CARGAR O CREAR HISTÓRICO ---
columnas_hist = ['Fecha_Captura', 'Fecha_Objetivo', 'Temp_OM', 'Lluvia_OM', 'Viento_OM', 
                 'Temp_AEMET', 'Lluvia_AEMET', 'Viento_AEMET', 'Temp_REAL', 'Lluvia_REAL', 'Viento_REAL']

if os.path.exists(archivo_txt):
    print("Fase 3: Cargando histórico existente...")
    df_historico = pd.read_csv(archivo_txt, sep='\t')
else:
    print("Fase 3: Creando nuevo histórico...")
    df_historico = pd.DataFrame(columns=columnas_hist)

# Añadimos las columnas REALES vacías a las nuevas 48h
df_nuevas['Temp_REAL'] = None
df_nuevas['Lluvia_REAL'] = None
df_nuevas['Viento_REAL'] = None

# Fusionamos lo viejo con las nuevas predicciones
df_total = pd.concat([df_historico, df_nuevas[columnas_hist]], ignore_index=True)

# --- FASE 4: DESCARGAR Y LEER DATOS REALES AEMET (CSV) ---
print("Fase 4: Descargando datos reales (CSV AEMET San Fernando)...")
url_reales_csv = 'https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_5972X_datos-horarios.csv?k=and&l=5972X&datos=det&w=0&f=temperatura&x='
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

try:
    respuesta_csv = requests.get(url_reales_csv, headers=headers)
    respuesta_csv.encoding = 'latin1'
    df_reales = pd.read_csv(io.StringIO(respuesta_csv.text), skiprows=4)
    
    # Limpiamos nombres de columnas y formateamos la fecha
    df_reales.columns = df_reales.columns.str.strip()
    df_reales['Fecha_Objetivo'] = pd.to_datetime(df_reales['Fecha y hora oficial'], format='%d/%m/%Y %H:%M').dt.strftime('%Y-%m-%d %H:00')
    
    # Rellenamos los datos reales en nuestro histórico donde coincida la Fecha_Objetivo
    for index, row in df_reales.iterrows():
        mask = df_total['Fecha_Objetivo'] == row['Fecha_Objetivo']
        if mask.any():
            df_total.loc[mask, 'Temp_REAL'] = float(row['Temperatura (ºC)']) if pd.notna(row['Temperatura (ºC)']) else None
            df_total.loc[mask, 'Viento_REAL'] = float(row['Velocidad del viento (km/h)']) if pd.notna(row['Velocidad del viento (km/h)']) else None
            df_total.loc[mask, 'Lluvia_REAL'] = float(row['Precipitación (mm)']) if pd.notna(row['Precipitación (mm)']) else None

except Exception as e:
    print(f"⚠️ Error AEMET CSV: {e}")

# --- FASE 6: GUARDAR ---
print("Fase 6: Limpiando y guardando...")
# Eliminamos duplicados de Fecha_Objetivo (nos quedamos con la captura más reciente)
df_total = df_total.drop_duplicates(subset=['Fecha_Objetivo'], keep='last')

# Ordenamos por Fecha_Objetivo cronológicamente
df_total = df_total.sort_values(by='Fecha_Objetivo')

# Guardamos el archivo
df_total.to_csv(archivo_txt, sep='\t', index=False, na_rep='')

print("✅ Proceso completado con éxito. Archivo historico.txt actualizado.")
