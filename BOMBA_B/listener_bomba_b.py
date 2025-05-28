import psycopg2
import select
import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

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

# URL base del backend para envío unificado
BASE_URL_B = os.environ.get('BASE_URL_B')
PREDICCION_URL = f"{BASE_URL_B}/predecir-bomba-b"

# Mapeo de nombre de canal a campo del modelo
CANAL_TO_CAMPO = {
    'canal_sensores_corriente_b': 'corriente_motor',
    'canal_excentricidad_bomba_b': 'excentricidad_bomba',
    'canal_flujo_descarga_b': 'flujo_descarga_ap',
    'canal_flujo_agua_domo_ap_b': 'flujo_agua_domo_ap',
    'canal_flujo_agua_domo_mp_b': 'flujo_agua_domo_mp',
    'canal_flujo_agua_recalentador_b': 'flujo_agua_recalentador',
    'canal_flujo_agua_vapor_alta_b': 'flujo_agua_vapor_alta',
    'canal_presion_agua_b': 'presion_agua_ap',
    'canal_temperatura_ambiental_b': 'temperatura_ambiental',
    'canal_temperatura_agua_alim_b': 'temperatura_agua_alim_ap',
    'canal_temperatura_estator_b': 'temperatura_estator',
    'canal_vibracion_axial_empuje_b': 'vibracion_axial',
    'canal_vibracion_x_descanso_b': 'vibracion_x_descanso',
    'canal_vibracion_y_descanso_b': 'vibracion_y_descanso',
    'canal_voltaje_barra_b': 'voltaje_barra'
}

CANALES = list(CANAL_TO_CAMPO.keys())

def conectar():
    """Establece una nueva conexión a la base de datos y configura los canales de escucha"""
    try:
        print("🔄 Conectando a la base de datos...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        for canal in CANALES:
            cur.execute(f"LISTEN {canal};")
            print(f"🟢 Escuchando canal '{canal}'")
        
        print("🟢 Conexión establecida.")
        return conn, cur
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        import time
        time.sleep(5)
        return None, None

def main():
    intentos = 0
    max_intentos = 10
    datos_sensores = {}

    while intentos < max_intentos:
        conn, cur = conectar()
        if not conn:
            intentos += 1
            print(f"⚠️ Reintentando... ({intentos}/{max_intentos})")
            continue
        intentos = 0

        try:
            while True:
                if select.select([conn], [], [], 10) == ([], [], []):
                    try:
                        cur.execute("SELECT 1")
                        continue
                    except psycopg2.OperationalError:
                        print("❌ Conexión perdida. Reconectando...")
                        break

                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    canal = notify.channel
                    payload = json.loads(notify.payload)
                    print(f"📡 Recibido en {canal}: {payload}")

                    if canal in CANAL_TO_CAMPO:
                        campo = CANAL_TO_CAMPO[canal]
                        datos_sensores[campo] = payload.get('valor')

                    # Verificar si tenemos todos los valores
                    if set(datos_sensores.keys()) == set(CANAL_TO_CAMPO.values()):
                        print("✅ Todos los datos recopilados, enviando a backend...")

                        try:
                            res = requests.post(PREDICCION_URL, json=datos_sensores)
                            print(f"🚀 Enviado a {PREDICCION_URL}: {res.status_code} - {res.text}")
                        except Exception as e:
                            print(f"❌ Error al enviar los datos al backend: {e}")

                        # Reiniciar el diccionario
                        datos_sensores = {}

        except Exception as e:
            print(f"❌ Error inesperado: {e}")

        # Cierre de conexión
        try:
            if cur and not cur.closed:
                cur.close()
            if conn and not conn.closed:
                conn.close()
            print("🔄 Conexión cerrada. Reconectando...")
        except:
            pass

        import time
        time.sleep(5)

if __name__ == '__main__':
    main()
