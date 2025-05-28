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

# URLs base del backend desde variables de entorno
BASE_URL = os.environ.get('BASE_URL')
BASE_URL_B = os.environ.get('BASE_URL_B')

# Mapeo de canales a rutas de endpoint - Exclusivo para bomba A
CANAL_ENDPOINTS = {
    # Canales originales de bomba A
    'canal_sensores_corriente': f"{BASE_URL}/prediccion_corriente",
    'canal_flujo_salida_12fpmfc': f"{BASE_URL}/prediccion_flujo-salida-12fpmfc",
    'canal_temperatura_estator': f"{BASE_URL}/prediccion_temperatura-estator",
    'canal_presion_succion_baa': f"{BASE_URL}/prediccion_presion-succion-baa",
    'canal_presion_agua_mp': f"{BASE_URL}/prediccion_presion-agua-mp",
    'canal_posicion_valvula_recirc': f"{BASE_URL}/prediccion_posicion-valvula-recirc",
    
    # Canales con rutas corregidas para bomba A
    'canal_voltaje_barra': f"{BASE_URL}/prediccion_voltaje-barra",
    'canal_vibracion_axial_descanso': f"{BASE_URL}/prediccion_vibracion-axial",
    'canal_temperatura_descanso_interno_bomba_1a': f"{BASE_URL}/prediccion_temp-descanso-bomba-1a",
    'canal_temperatura_descanso_interna_motor_bomba_1a': f"{BASE_URL}/prediccion_temp-motor-bomba-1a",
    
    # Canales para flujos de agua y otros de bomba A
    'canal_flujo_agua_vapor_alta': f"{BASE_URL}/prediccion_flujo-agua-vapor-alta",
    'canal_flujo_agua_recalentador': f"{BASE_URL}/prediccion_flujo-agua-recalentador",
    'canal_flujo_agua_domo_mp': f"{BASE_URL}/prediccion_flujo-agua-domo-mp",
    'canal_flujo_agua_domo_ap': f"{BASE_URL}/prediccion_flujo-agua-domo-ap",
    'canal_excentricidad_bomba': f"{BASE_URL}/prediccion_excentricidad-bomba",

    # Otros canales de bomba A
    'canal_salida_agua': f"{BASE_URL}/prediccion_salida-agua",
    'canal_presion_agua': f"{BASE_URL}/prediccion_presion-agua",
    'canal_mw_brutos_gas': f"{BASE_URL}/prediccion_mw-brutos-gas",
    'canal_temp_empuje_bomba_1a': f"{BASE_URL}/prediccion_temp-empuje-bomba-1a",
}

# Nota: Eliminamos la referencia a los canales de bomba B ya que ahora está en un archivo separado

# Lista de canales a escuchar para bomba A
CANALES = list(CANAL_ENDPOINTS.keys())

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Suscribirse a todos los canales definidos
    for canal in CANALES:
        cur.execute(f"LISTEN {canal};")
        print(f"🟢 Escuchando canal '{canal}'...")

    print("⚡ Listener iniciado y esperando notificaciones...")

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
                # Obtener el endpoint para este canal
                endpoint = CANAL_ENDPOINTS.get(canal)
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
