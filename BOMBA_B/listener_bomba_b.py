import psycopg2
import select
import json
import requests
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
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

# Preservar la compatibilidad con endpoints individuales
CANAL_ENDPOINTS = {
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
        print("Conectando a la base de datos...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        for canal in CANALES:
            cur.execute(f"LISTEN {canal};")
            print(f"Escuchando canal '{canal}'")
        
        print("Conexión establecida.")
        print("Listener de Bomba B iniciado y esperando notificaciones...")
        return conn, cur
    except Exception as e:
        print(f"Error de conexión: {e}")
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
            print(f"Reintentando... ({intentos}/{max_intentos})")
            continue
        intentos = 0

        try:
            while True:
                if select.select([conn], [], [], 10) == ([], [], []):
                    try:
                        cur.execute("SELECT 1")
                        continue
                    except psycopg2.OperationalError:
                        print("Conexión perdida. Reconectando...")
                        break

                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    canal = notify.channel
                    payload = json.loads(notify.payload)
                    print(f"Recibido en {canal}: {payload}")

                    if canal in CANAL_TO_CAMPO:
                        campo = CANAL_TO_CAMPO[canal]
                        datos_sensores[campo] = payload.get('valor')
                        
                        # Enviar a endpoint individual si existe
                        if canal in CANAL_ENDPOINTS:
                            try:
                                endpoint = CANAL_ENDPOINTS[canal]
                                data = {
                                    'id_sensor': payload.get('id_sensor'),
                                    'valor': payload.get('valor')
                                }
                                print(f"Enviando a endpoint individual: {endpoint}")
                                response = requests.post(endpoint, json=data)
                                print(f"Respuesta: {response.status_code}")
                            except Exception as e:
                                print(f"Error enviando a endpoint individual: {e}")

                    # Verificar si tenemos todos los valores para enviar al endpoint de predicción
                    required_fields = [
                        'corriente_motor', 'excentricidad_bomba', 'flujo_descarga_ap',
                        'flujo_agua_domo_ap', 'flujo_agua_domo_mp', 'flujo_agua_recalentador',
                        'flujo_agua_vapor_alta', 'presion_agua_ap', 'temperatura_ambiental', 
                        'temperatura_agua_alim_ap', 'temperatura_estator', 'vibracion_axial',
                        'vibracion_x_descanso', 'vibracion_y_descanso', 'voltaje_barra'
                    ]
                    
                    # Verificar cuántos campos tenemos
                    campos_presentes = [field for field in required_fields if field in datos_sensores]
                    campos_faltantes = [field for field in required_fields if field not in datos_sensores]
                    
                    print(f"Estado actual: {len(campos_presentes)}/{len(required_fields)} campos recopilados")
                    
                    if len(campos_presentes) == len(required_fields):  # Solo enviamos cuando tenemos TODOS los campos
                        print(f"Datos suficientes recopilados ({len(campos_presentes)}/{len(required_fields)}), enviando a backend...")
                        if campos_faltantes:
                            print(f"Campos faltantes: {campos_faltantes}")

                        try:
                            # Mostrar los datos que se van a enviar
                            print("Enviando datos:")
                            
                            for campo, valor in datos_sensores.items():
                                print(f"  - {campo}: {valor}")
                                
                            # Preparar los datos para enviar - debe coincidir con PrediccionBombaBInput
                            datos_a_enviar = datos_sensores
                            # No incluimos timestamp ya que no está en el esquema PrediccionBombaBInput
                            
                            res = requests.post(PREDICCION_URL, json=datos_a_enviar)
                            if res.status_code == 200:
                                print(f"Predicción unificada enviada exitosamente: {res.status_code}")
                                try:
                                    print(f"  Respuesta: {res.json()}")
                                except:
                                    print(f"  Respuesta: {res.text[:100] if res.text else 'Sin contenido'}")
                                # Reiniciar el diccionario después del envío exitoso
                                datos_sensores = {}
                            else:
                                print(f"Error {res.status_code} al enviar la predicción unificada")
                                print(f"  Respuesta: {res.text[:200] if res.text else 'Sin contenido'}")
                        except Exception as e:
                            print(f"Error al enviar la predicción unificada: {e}")
                            import traceback
                            traceback.print_exc()

        except Exception as e:
            print(f"Error inesperado: {e}")

        # Cierre de conexión
        try:
            if cur and not cur.closed:
                cur.close()
            if conn and not conn.closed:
                conn.close()
            print("Conexión cerrada. Reconectando...")
        except:
            pass

        import time
        time.sleep(5)

# Definir un manejador HTTP simple
class SimpleHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Manejador para peticiones GET"""
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"<html><body><h1>Listener de Bomba B funcionando</h1><p>El servicio esta activo y escuchando notificaciones PostgreSQL.</p></body></html>")
    
    def log_message(self, format, *args):
        """Sobrescribir el método de logging para evitar mensajes excesivos"""
        return

def run_http_server():
    """Función para ejecutar el servidor HTTP en un hilo separado"""
    # Obtener el puerto del entorno o usar 8080 por defecto (requerido por IBM Cloud Engine)
    port = int(os.environ.get('PORT', 8080))
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHTTPHandler)
    print(f"Servidor HTTP iniciado en el puerto {port}")
    httpd.serve_forever()

if __name__ == '__main__':
    # Iniciar el servidor HTTP en un hilo separado
    http_thread = threading.Thread(target=run_http_server)
    http_thread.daemon = True  # El hilo terminará cuando el programa principal termine
    http_thread.start()
    
    # Ejecutar el listener en el hilo principal
    main()
