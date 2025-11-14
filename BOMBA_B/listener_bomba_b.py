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
    intentos_conexion = 0
    max_intentos_inicial = 10
    reconexiones_consecutivas = 0
    max_reconexiones = 50  # Límite para reconexiones durante ejecución
    tiempo_espera_base = 5
    # Modificación: cambiar a un diccionario donde cada clave es un tiempo_sensor
    datos_por_tiempo = {}

    while True:
        conn, cur = conectar()
        if not conn:
            intentos_conexion += 1
            reconexiones_consecutivas += 1

            # Aplicar backoff exponencial con límite máximo de 60 segundos
            tiempo_espera = min(tiempo_espera_base * (2 ** min(intentos_conexion - 1, 4)), 60)

            print(f"Reintento {intentos_conexion} (reconexiones consecutivas: {reconexiones_consecutivas}/{max_reconexiones})")
            print(f"Esperando {tiempo_espera} segundos antes del próximo intento...")

            # Verificar si hemos excedido los límites
            if intentos_conexion >= max_intentos_inicial and reconexiones_consecutivas >= max_reconexiones:
                print(f"Se alcanzó el límite de {max_reconexiones} reconexiones consecutivas fallidas. Deteniendo...")
                break

            import time
            time.sleep(tiempo_espera)
            continue

        # Reiniciar contadores al conectar exitosamente
        intentos_conexion = 0
        reconexiones_consecutivas = 0
        print("Conexión exitosa. Contadores de reintento reiniciados.")

        try:
            while True:
                if select.select([conn], [], [], 10) == ([], [], []):
                    try:
                        cur.execute("SELECT 1")
                        continue
                    except psycopg2.OperationalError as e:
                        print(f"Conexión perdida (heartbeat falló): {e}")
                        print("Reconectando...")
                        reconexiones_consecutivas += 1
                        break

                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    canal = notify.channel
                    payload = json.loads(notify.payload)
                    print(f"Recibido en {canal}: {payload}")

                    # Extraer tiempo_sensor del payload (solo para agrupación)
                    tiempo_sensor = payload.get('tiempo_sensor')
                    
                    if not tiempo_sensor:
                        print(f"ADVERTENCIA: Notificación sin tiempo_sensor: {payload}")
                        continue

                    # Inicializar el diccionario para este tiempo si no existe
                    if tiempo_sensor not in datos_por_tiempo:
                        datos_por_tiempo[tiempo_sensor] = {}

                    if canal in CANAL_TO_CAMPO:
                        campo = CANAL_TO_CAMPO[canal]
                        datos_por_tiempo[tiempo_sensor][campo] = payload.get('valor')
                        
                        # Enviar a endpoint individual si existe (sin el tiempo_sensor)
                        if canal in CANAL_ENDPOINTS:
                            try:
                                endpoint = CANAL_ENDPOINTS[canal]
                                # Solo enviar id_sensor y valor
                                data = {
                                    'id_sensor': payload.get('id_sensor'),
                                    'valor': payload.get('valor')
                                }
                                print(f"Enviando a endpoint individual: {endpoint}")
                                response = requests.post(endpoint, json=data, timeout=30)
                                print(f"Respuesta: {response.status_code}")
                            except Exception as e:
                                print(f"Error enviando a endpoint individual: {e}")

                    # Lista de todos los campos requeridos
                    required_fields = [
                        'corriente_motor', 'excentricidad_bomba', 'flujo_descarga_ap',
                        'flujo_agua_domo_ap', 'flujo_agua_domo_mp', 'flujo_agua_recalentador',
                        'flujo_agua_vapor_alta', 'presion_agua_ap', 'temperatura_ambiental', 
                        'temperatura_agua_alim_ap', 'temperatura_estator', 'vibracion_axial',
                        'vibracion_x_descanso', 'vibracion_y_descanso', 'voltaje_barra'
                    ]
                    
                    # Verificar los datos para el tiempo actual
                    datos_sensores = datos_por_tiempo[tiempo_sensor]
                    campos_presentes = [field for field in required_fields if field in datos_sensores]
                    campos_faltantes = [field for field in required_fields if field not in datos_sensores]
                    
                    print(f"Estado para tiempo {tiempo_sensor}: {len(campos_presentes)}/{len(required_fields)} campos recopilados")
                    
                    # Si tenemos todos los campos para este tiempo, enviamos a la predicción
                    if len(campos_presentes) == len(required_fields):
                        print(f"Datos suficientes recopilados para tiempo {tiempo_sensor} ({len(campos_presentes)}/{len(required_fields)}), enviando a backend...")
                        
                        try:
                            # Mostrar los datos que se van a enviar
                            print(f"Enviando datos para tiempo {tiempo_sensor}:")
                            
                            for campo, valor in datos_sensores.items():
                                print(f"  - {campo}: {valor}")
                                
                            # Preparar los datos para enviar (sin incluir tiempo_sensor)
                            datos_a_enviar = datos_sensores.copy()
                            
                            res = requests.post(PREDICCION_URL, json=datos_a_enviar, timeout=60)
                            if res.status_code == 200:
                                print(f"Predicción unificada enviada exitosamente para tiempo {tiempo_sensor}: {res.status_code}")
                                try:
                                    print(f"  Respuesta: {res.json()}")
                                except:
                                    print(f"  Respuesta: {res.text[:100] if res.text else 'Sin contenido'}")
                                # Eliminar este conjunto de datos después del envío exitoso
                                del datos_por_tiempo[tiempo_sensor]
                            else:
                                print(f"Error {res.status_code} al enviar la predicción unificada para tiempo {tiempo_sensor}")
                                print(f"  Respuesta: {res.text[:200] if res.text else 'Sin contenido'}")
                        except Exception as e:
                            print(f"Error al enviar la predicción unificada para tiempo {tiempo_sensor}: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    # Limpieza de conjuntos antiguos incompletos
                    if len(datos_por_tiempo) > 10:
                        print("Limpiando conjuntos de datos antiguos incompletos...")
                        tiempos_ordenados = sorted(datos_por_tiempo.keys())
                        for t in tiempos_ordenados[:-10]:
                            if len(datos_por_tiempo[t]) < len(required_fields):
                                print(f"Eliminando conjunto incompleto para tiempo {t} con {len(datos_por_tiempo[t])}/{len(required_fields)} campos")
                                del datos_por_tiempo[t]

        except Exception as e:
            print(f"Error inesperado: {e}")
            import traceback
            traceback.print_exc()
            reconexiones_consecutivas += 1

        # Cierre de conexión
        try:
            if cur and not cur.closed:
                cur.close()
            if conn and not conn.closed:
                conn.close()
            print("Conexión cerrada. Reconectando...")
        except Exception as e:
            print(f"Error al cerrar conexión: {e}")

        # Backoff exponencial antes de reconectar
        tiempo_espera = min(tiempo_espera_base * (2 ** min(reconexiones_consecutivas - 1, 4)), 60)
        print(f"Esperando {tiempo_espera} segundos antes de reconectar...")
        import time
        time.sleep(tiempo_espera)

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
