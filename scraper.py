import pandas as pd
import requests
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from io import StringIO

def acumula_datos_diario():
    print("Iniciando proceso de actualización del histórico...")
    
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    archivo_txt = os.path.join(directorio_actual, 'historico.txt')
    KT_TO_KMH = 1.852
    
    hora_base = datetime.now().replace(minute=0, second=0, microsecond=0)
    fechas_objetivo_dt = [hora_base + timedelta(hours=i) for i in range(1, 49)]
    
    # --- FASE 1: OPEN-METEO ---
    url_om = 'https://api.open-meteo.com/v1/forecast?latitude=36.4665&longitude=-6.1962&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FMadrid'
    dict_om = {}
    try:
        r_om = requests.get(url_om)
        datos_om = r_om.json()
        df_om = pd.DataFrame(datos_om['hourly'])
        df_om['time'] = pd.to_datetime(df_om['time'])
        dict_om = df_om.set_index('time').to_dict('index')
    except: print("⚠️ Fallo en Open-Meteo")

    # --- FASE 2: AEMET PREDICCIONES ---
    url_aemet_pred = 'https://www.aemet.es/xml/municipios_h/localidad_h_11031.xml'
    temp_aemet, lluvia_aemet, viento_aemet = {}, {}, {}
    try:
        r_ae = requests.get(url_aemet_pred)
        r_ae.encoding = 'utf-8'
        root = ET.fromstring(r_ae.content)
        for dia in root.findall('.//dia'):
            fecha = dia.get('fecha')
            for t in dia.findall('temperatura'):
                h = t.get('periodo')
                dt_str = datetime.strptime(f"{fecha} {h}", '%Y-%m-%d %H').strftime('%d-%m-%Y_%H:00')
                temp_aemet[dt_str] = float(t.text) if t.text else None
            for p in dia.findall('precipitacion'):
                h = p.get('periodo')
                dt_str = datetime.strptime(f"{fecha} {h}", '%Y-%m-%d %H').strftime('%d-%m-%Y_%H:00')
                val = p.text if p.text != 'Ip' else '0.05'
                lluvia_aemet[dt_str] = float(val) if val else 0.0
            for v in dia.findall('viento'):
                h = v.get('periodo')
                dt_str = datetime.strptime(f"{fecha} {h}", '%Y-%m-%d %H').strftime('%d-%m-%Y_%H:00')
                vel = v.find('velocidad').text if v.find('velocidad') is not None else None
                viento_aemet[dt_str] = float(vel) if vel else None
    except: print("⚠️ Fallo en AEMET")

    # --- FASE 3: CARGAR Y LIMPIAR ---
    cols_validas = ['Fecha_Captura', 'Fecha_Objetivo', 'Temp_OM', 'Lluvia_OM', 'Viento_OM', 
                    'Temp_AEMET', 'Lluvia_AEMET', 'Viento_AEMET', 'Temp_REAL', 'Lluvia_REAL', 'Viento_REAL']
    
    if os.path.exists(archivo_txt):
        # Leemos ignorando errores de columnas extra
        historico = pd.read_csv(archivo_txt, sep='\t')
        # ELIMINAR COLUMNAS "UNNAMED" O BASURA
        historico = historico[[c for c in historico.columns if c in cols_validas]]
    else:
        historico = pd.DataFrame(columns=cols_validas)

    # --- FASE 4: DATOS REALES ---
    url_reales_csv = 'https://www.aemet.es/es/eltiempo/observation/ultimosdatos_5972X_datos-horarios.csv?k=and&l=5972X&datos=det&w=0&f=temperatura&x='
    try:
        r_csv = requests.get(url_reales_csv)
        r_csv.encoding = 'latin-1'
        df_reales = pd.read_csv(StringIO("\n".join(r_csv.text.splitlines()[4:]))) # Saltamos cabecera basura
        df_reales.columns = [c.strip() for c in df_reales.columns]
        for _, row in df_reales.iterrows():
            try:
                f_obj_str = datetime.strptime(str(row.iloc[0]).strip(), '%d/%m/%Y %H:%M').strftime('%d-%m-%Y_%H:00')
                if f_obj_str in historico['Fecha_Objetivo'].values:
                    idx = historico['Fecha_Objetivo'] == f_obj_str
                    historico.loc[idx, 'Temp_REAL'] = float(row.iloc[1])
                    historico.loc[idx, 'Viento_REAL'] = round(float(row.iloc[2]) * KT_TO_KMH, 2)
                    historico.loc[idx, 'Lluvia_REAL'] = float(row.iloc[6])
            except: continue
    except: print("⚠️ Sin datos reales nuevos")

    # --- FASE 5: NUEVAS FILAS ---
    fecha_cap = datetime.now().strftime('%d-%m-%Y_%H:%M')
    nuevas = []
    for dt in fechas_objetivo_dt:
        f_obj = dt.strftime('%d-%m-%Y_%H:00')
        val_om = dict_om.get(dt, {})
        nuevas.append({
            'Fecha_Captura': fecha_cap, 'Fecha_Objetivo': f_obj,
            'Temp_OM': val_om.get('temperature_2m'), 'Lluvia_OM': val_om.get('precipitation'), 'Viento_OM': val_om.get('wind_speed_10m'),
            'Temp_AEMET': temp_aemet.get(f_obj), 'Lluvia_AEMET': lluvia_aemet.get(f_obj), 'Viento_AEMET': viento_aemet.get(f_obj),
            'Temp_REAL': None, 'Lluvia_REAL': None, 'Viento_REAL': None
        })
    
    historico = pd.concat([historico, pd.DataFrame(nuevas)], ignore_index=True)
    historico = historico.drop_duplicates(subset=['Fecha_Objetivo'], keep='last')

    # --- FASE 6: ORDENAR Y GUARDAR LIMPIO ---
    historico['temp_dt'] = pd.to_datetime(historico['Fecha_Objetivo'], format='%d-%m-%Y_%H:00')
    historico = historico.sort_values('temp_dt').drop(columns=['temp_dt'])
    
    # Aseguramos que solo guardamos las columnas que queremos y en ese orden
    historico = historico[cols_validas]
    
    historico.to_csv(archivo_txt, sep='\t', index=False, na_rep='')
    print(f"✅ Histórico saneado y guardado.")

if __name__ == "__main__":
    acumula_datos_diario()
