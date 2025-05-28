import psycopg2
import select
import json
import requests
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de la base de datos usando variables de entorno
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT'))
}

# URL base del backend desde variables de entorno (usar solo la B)
BASE_URL_B = os.environ.get('BASE_URL_B')

# Mapeo de canales exclusivamente para bomba B (usando las rutas exactas del backend)
CANAL_ENDPOINTS_B = {
    'canal_sensores_corriente_b': f"{BASE_URL_B}/prediccion_corriente",
    'canal_excentricidad_bomba_b': f"{BASE_URL_B}/prediccion_excentricidad_bomba",
    'canal_flujo_descarga_b': f"{BASE_URL_B}/prediccion_flujo_descarga",
    'canal_flujo_agua_domo_ap_b': f"{BASE_URL_B}/prediccion_flujo_agua_domo_ap",
    'canal_flujo_agua_domo_mp_b': f"{BASE_URL_B}/prediccion_flujo_agua_domo_mp",
    'canal_flujo_agua_recalentador_b': f"{BASE_URL_B}/prediccion_flujo_agua_recalentador",
    'canal_flujo_agua_vapor_alta_b': f"{BASE_URL_B}/prediccion_flujo_agua_vapor_alta",
    'canal_presion_agua_b': f"{BASE_URL_B}/prediccion_presion_agua",
    'canal_temperatura_ambiental_b': f"{BASE_URL_B}/prediccion_temperatura_ambiental",
    'canal_temperatura_agua_alim_b': f"{BASE_URL_B}/prediccion_temperatura_agua_alim",
    'canal_temperatura_estator_b': f"{BASE_URL_B}/prediccion_temperatura_estator",
    'canal_vibracion_axial_empuje_b': f"{BASE_URL_B}/prediccion_vibracion_axial_empuje",
    'canal_vibracion_x_descanso_b': f"{BASE_URL_B}/prediccion_vibracion_x_descanso",
    'canal_vibracion_y_descanso_b': f"{BASE_URL_B}/prediccion_vibracion_y_descanso",
    'canal_voltaje_barra_b': f"{BASE_URL_B}/prediccion_voltaje_barra",
}

# Lista de canales a escuchar
CANALES = list(CANAL_ENDPOINTS_B.keys())

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Suscribirse a todos los canales definidos
    for canal in CANALES:
        cur.execute(f"LISTEN {canal};")
        print(f"🟢 Escuchando canal '{canal}'...")

    print("⚡ Listener de Bomba B iniciado y esperando notificaciones...")

    while True:
        if select.select([conn], [], [], 10) == ([], [], []):
            continue
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            canal = notify.channel
            payload = json.loads(notify.payload)
            print(f"📡 Recibido en {canal}: {payload}")

            # Enviar al backend usando la ruta correspondiente al canal
            try:
                # Obtener el endpoint correspondiente al canal
                endpoint = CANAL_ENDPOINTS_B.get(canal)
                if not endpoint:
                    print(f"⚠️ No se encontró endpoint para el canal {canal}")
                    continue
                    
                # Enviar los datos al endpoint específico
                res = requests.post(endpoint, json=payload)
                print(f"✅ Enviado a {endpoint}: {res.status_code}")
            except Exception as e:
                print(f"❌ Error al enviar al backend: {e}")

if __name__ == '__main__':
    main()
