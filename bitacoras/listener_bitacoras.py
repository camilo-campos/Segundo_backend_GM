"""
Listener para clasificacion automatica de bitacoras GM
Escucha los canales canal_gm_bitacora_a y canal_gm_bitacora_b
y envia las bitacoras al backend principal para clasificacion con LLM
"""

import psycopg2
import select
import json
import requests
import os
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from datetime import datetime

# Cargar variables de entorno desde .env
load_dotenv()

# Configuracion de la base de datos
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT', 30226))
}

# URL base del backend principal
BASE_URL = os.environ.get('BASE_URL', 'https://backend-gm.1tfr3xva5g42.us-south.codeengine.appdomain.cloud')
# Quitar /sensores si viene en la URL
if BASE_URL.endswith('/sensores'):
    BASE_URL = BASE_URL.replace('/sensores', '')
if BASE_URL.endswith('/sensores_b'):
    BASE_URL = BASE_URL.replace('/sensores_b', '')

CLASIFICAR_URL = f"{BASE_URL}/gm-bitacoras/clasificar"

# API Key para autenticacion
API_KEY = os.environ.get('API_KEY', 'gm-internal-service-key-2025')
HEADERS = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY
}

# Canales a escuchar
CANALES = ['canal_gm_bitacora_a', 'canal_gm_bitacora_b']

# Mapeo canal -> tabla
CANAL_TO_TABLA = {
    'canal_gm_bitacora_a': 'a',
    'canal_gm_bitacora_b': 'b'
}


def conectar():
    """Establece conexion a la base de datos y configura los canales"""
    try:
        print(f"[{datetime.now()}] Conectando a la base de datos...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        for canal in CANALES:
            cur.execute(f"LISTEN {canal};")
            print(f"[{datetime.now()}] Escuchando canal '{canal}'")

        print(f"[{datetime.now()}] Conexion establecida.")
        print(f"[{datetime.now()}] Listener de Bitacoras GM iniciado y esperando notificaciones...")
        print(f"[{datetime.now()}] Endpoint de clasificacion: {CLASIFICAR_URL}")
        return conn, cur
    except Exception as e:
        print(f"[{datetime.now()}] Error de conexion: {e}")
        time.sleep(5)
        return None, None


def clasificar_bitacora(id_bitacora, texto_bitacora, tabla):
    """Envia una bitacora al backend para clasificacion"""
    try:
        data = {
            'id': id_bitacora,
            'bitacora': texto_bitacora,
            'tabla': tabla
        }
        print(f"[{datetime.now()}] Enviando bitacora {id_bitacora} (tabla {tabla}) a clasificar...")
        print(f"[{datetime.now()}]   Texto: {texto_bitacora[:80]}...")

        response = requests.post(CLASIFICAR_URL, json=data, headers=HEADERS, timeout=120)

        if response.status_code == 200:
            resultado = response.json()
            print(f"[{datetime.now()}] Bitacora {id_bitacora} clasificada exitosamente")
            print(f"[{datetime.now()}]   Clasificacion: {resultado.get('clasificacion', 'N/A')}")
            if resultado.get('alerta_aviso'):
                print(f"[{datetime.now()}]   Alerta: {resultado.get('alerta_aviso', '')[:50]}...")
            return True
        else:
            print(f"[{datetime.now()}] Error {response.status_code} al clasificar bitacora {id_bitacora}")
            print(f"[{datetime.now()}]   Respuesta: {response.text[:200]}")
            return False

    except requests.exceptions.Timeout:
        print(f"[{datetime.now()}] Timeout al clasificar bitacora {id_bitacora}")
        return False
    except Exception as e:
        print(f"[{datetime.now()}] Error al clasificar bitacora {id_bitacora}: {e}")
        return False


def main():
    """Funcion principal del listener"""
    intentos_conexion = 0
    max_intentos = 50
    tiempo_espera_base = 5

    while True:
        conn, cur = conectar()
        if not conn:
            intentos_conexion += 1
            tiempo_espera = min(tiempo_espera_base * (2 ** min(intentos_conexion - 1, 4)), 60)
            print(f"[{datetime.now()}] Reintento {intentos_conexion}/{max_intentos}")

            if intentos_conexion >= max_intentos:
                print(f"[{datetime.now()}] Maximo de intentos alcanzado. Deteniendo...")
                break

            time.sleep(tiempo_espera)
            continue

        # Reiniciar contador al conectar
        intentos_conexion = 0
        print(f"[{datetime.now()}] Conexion exitosa. Esperando bitacoras...")

        try:
            while True:
                # Esperar notificaciones con timeout de 30 segundos
                if select.select([conn], [], [], 30) == ([], [], []):
                    # Heartbeat - verificar conexion
                    try:
                        cur.execute("SELECT 1")
                        continue
                    except psycopg2.OperationalError as e:
                        print(f"[{datetime.now()}] Conexion perdida: {e}")
                        break

                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    canal = notify.channel

                    try:
                        payload = json.loads(notify.payload)
                        print(f"[{datetime.now()}] Notificacion recibida en {canal}")

                        id_bitacora = payload.get('id')
                        texto_bitacora = payload.get('bitacora')
                        tabla = CANAL_TO_TABLA.get(canal, 'a')

                        if id_bitacora and texto_bitacora:
                            clasificar_bitacora(id_bitacora, texto_bitacora, tabla)
                        else:
                            print(f"[{datetime.now()}] Payload incompleto: {payload}")

                    except json.JSONDecodeError as e:
                        print(f"[{datetime.now()}] Error parseando payload: {e}")
                    except Exception as e:
                        print(f"[{datetime.now()}] Error procesando notificacion: {e}")

        except Exception as e:
            print(f"[{datetime.now()}] Error inesperado: {e}")
            import traceback
            traceback.print_exc()

        # Cerrar conexion
        try:
            if cur and not cur.closed:
                cur.close()
            if conn and not conn.closed:
                conn.close()
            print(f"[{datetime.now()}] Conexion cerrada. Reconectando...")
        except Exception as e:
            print(f"[{datetime.now()}] Error al cerrar conexion: {e}")

        time.sleep(5)


# Servidor HTTP simple para health checks
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"<html><body><h1>Listener Bitacoras GM</h1><p>Servicio activo escuchando notificaciones PostgreSQL.</p></body></html>")

    def log_message(self, format, *args):
        return  # Silenciar logs HTTP


def run_http_server():
    """Ejecuta servidor HTTP para health checks"""
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('', port), HealthHandler)
    print(f"[{datetime.now()}] Servidor HTTP iniciado en puerto {port}")
    server.serve_forever()


if __name__ == '__main__':
    # Iniciar servidor HTTP en hilo separado
    http_thread = threading.Thread(target=run_http_server)
    http_thread.daemon = True
    http_thread.start()

    # Ejecutar listener en hilo principal
    main()
