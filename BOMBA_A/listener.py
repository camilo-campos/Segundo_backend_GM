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

# URLs base del backend desde variables de entorno
BASE_URL = os.environ.get('BASE_URL')
PREDICCION_URL = f"{BASE_URL}/predecir-bomba"

# Mapeo de nombre de canal a campo del modelo para bomba A
CANAL_TO_CAMPO = {
    # Canales originales de bomba A
    'canal_sensores_corriente': 'corriente_motor',
    'canal_flujo_salida_12fpmfc': 'flujo_salida_12fpmfc',
    'canal_temperatura_estator': 'temperatura_estator',
    'canal_presion_succion_baa': 'presion_succion_baa',
    'canal_presion_agua_mp': 'presion_agua_mp',
    'canal_posicion_valvula_recirc': 'posicion_valvula_recirc',
    
    # Canales con rutas corregidas para bomba A
    'canal_voltaje_barra': 'voltaje_barra',
    'canal_vibracion_axial_descanso': 'vibracion_axial',
    'canal_temperatura_descanso_interno_bomba_1a': 'temp_bomba',
    'canal_temperatura_descanso_interna_motor_bomba_1a': 'temp_motor',
    
    # Canales para flujos de agua y otros de bomba A
    'canal_flujo_agua_vapor_alta': 'flujo_agua',
    'canal_flujo_agua_recalentador': 'flujo_agua_recalentador',
    'canal_flujo_agua_domo_mp': 'flujo_agua_domo_mp',
    'canal_flujo_agua_domo_ap': 'flujo_agua_domo_ap',
    'canal_excentricidad_bomba': 'excentricidad_bomba',

    # Otros canales de bomba A
    'canal_salida_agua': 'salida_bomba',
    'canal_presion_agua': 'presion_agua',
    'canal_mw_brutos_gas': 'mw_brutos_gas',
    'canal_temp_empuje_bomba_1a': 'temp_empuje',
    
    # Canal para temperatura ambiental
    'canal_temperatura_ambiental': 'temp_ambiental',
}

# Preservar la compatibilidad con endpoints individuales (opcional)
CANAL_ENDPOINTS = {
    'canal_sensores_corriente': f"{BASE_URL}/prediccion_corriente",
    'canal_flujo_salida_12fpmfc': f"{BASE_URL}/prediccion_flujo-salida-12fpmfc",
    'canal_temperatura_estator': f"{BASE_URL}/prediccion_temperatura-estator",
    'canal_presion_succion_baa': f"{BASE_URL}/prediccion_presion-succion-baa",
    'canal_presion_agua_mp': f"{BASE_URL}/prediccion_presion-agua-mp",
    'canal_posicion_valvula_recirc': f"{BASE_URL}/prediccion_posicion-valvula-recirc",
    'canal_voltaje_barra': f"{BASE_URL}/prediccion_voltaje-barra",
    'canal_vibracion_axial_descanso': f"{BASE_URL}/prediccion_vibracion-axial",
    'canal_temperatura_descanso_interno_bomba_1a': f"{BASE_URL}/prediccion_temp-descanso-bomba-1a",
    'canal_temperatura_descanso_interna_motor_bomba_1a': f"{BASE_URL}/prediccion_temp-motor-bomba-1a",
    'canal_flujo_agua_vapor_alta': f"{BASE_URL}/prediccion_flujo-agua-vapor-alta",
    'canal_flujo_agua_recalentador': f"{BASE_URL}/prediccion_flujo-agua-recalentador",
    'canal_flujo_agua_domo_mp': f"{BASE_URL}/prediccion_flujo-agua-domo-mp",
    'canal_flujo_agua_domo_ap': f"{BASE_URL}/prediccion_flujo-agua-domo-ap",
    'canal_excentricidad_bomba': f"{BASE_URL}/prediccion_excentricidad-bomba",
    'canal_salida_agua': f"{BASE_URL}/prediccion_salida-agua",
    'canal_presion_agua': f"{BASE_URL}/prediccion_presion-agua",
    'canal_mw_brutos_gas': f"{BASE_URL}/prediccion_mw-brutos-gas",
    'canal_temp_empuje_bomba_1a': f"{BASE_URL}/prediccion_temp-empuje-bomba-1a",
    'canal_temperatura_ambiental': f"{BASE_URL}/prediccion_temperatura-ambiental",
}

# Lista de canales a escuchar para bomba A
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
        print("Listener de Bomba A iniciado y esperando notificaciones...")
        return conn, cur
    except Exception as e:
        print(f"Error de conexión: {e}")
        import time
        time.sleep(5)
        return None, None

def main():
    intentos = 0
    max_intentos = 10
    # Modificación: cambiar a un diccionario donde cada clave es un tiempo_sensor
    datos_por_tiempo = {}

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
                    
                    # Extraer tiempo_sensor del payload (solo para agrupación)
                    tiempo_sensor = payload.get('tiempo_sensor')
                    
                    if not tiempo_sensor:
                        print(f"ADVERTENCIA: Notificación sin tiempo_sensor: {payload}")
                        continue

                    # Inicializar el diccionario para este tiempo si no existe
                    if tiempo_sensor not in datos_por_tiempo:
                        datos_por_tiempo[tiempo_sensor] = {}
                    
                    # Almacenar el valor en nuestro diccionario de datos
                    if canal in CANAL_TO_CAMPO:
                        campo = CANAL_TO_CAMPO[canal]
                        datos_por_tiempo[tiempo_sensor][campo] = payload.get('valor')
                        print(f"Guardado '{campo}' para tiempo {tiempo_sensor}: {payload.get('valor')}")
                        
                        # Opcional: enviar también a la ruta individual (sin el tiempo_sensor)
                        try:
                            endpoint = CANAL_ENDPOINTS.get(canal)
                            if endpoint:
                                # Solo enviamos id_sensor y valor
                                data = {
                                    'id_sensor': payload.get('id_sensor'),
                                    'valor': payload.get('valor')
                                }
                                res = requests.post(endpoint, json=data, timeout=30)
                                print(f"Enviado a endpoint individual {endpoint}: {res.status_code}")
                        except Exception as e:
                            print(f"Error al enviar a endpoint individual: {e}")


                    # Verificar si tenemos suficientes datos para enviar a la predicción unificada
                    # Lista de campos requeridos según PrediccionBombaInput
                    campos_requeridos = [
                        'presion_agua', 'voltaje_barra', 'corriente_motor', 'vibracion_axial',
                        'salida_bomba', 'flujo_agua', 'mw_brutos_gas', 'temp_motor',
                        'temp_bomba', 'temp_empuje', 'temp_ambiental', 'excentricidad_bomba',
                        'flujo_agua_domo_ap', 'flujo_agua_domo_mp', 'flujo_agua_recalentador',
                        'posicion_valvula_recirc', 'presion_agua_mp', 'presion_succion_baa',
                        'temperatura_estator', 'flujo_salida_12fpmfc'
                    ]
                    
                    # Verificar los datos para el tiempo actual
                    datos_sensores = datos_por_tiempo[tiempo_sensor]
                    campos_presentes = [campo for campo in campos_requeridos if campo in datos_sensores]
                    campos_faltantes = [campo for campo in campos_requeridos if campo not in datos_sensores]
                    
                    print(f"Estado para tiempo {tiempo_sensor}: {len(campos_presentes)}/{len(campos_requeridos)} campos recopilados")
                    
                    # Si tenemos al menos 15 campos, intentamos enviar con valores por defecto para los faltantes
                    if len(campos_presentes) >= 15:
                        print(f"Datos suficientes recopilados para tiempo {tiempo_sensor}, enviando a predicción unificada...")
                        
                        # Valores por defecto para campos comunes que podrían faltar
                        valores_por_defecto = {
                            'temp_ambiental': 25.0,
                            'mw_brutos_gas': 100.0,
                            'salida_bomba': 1000.0,
                            'flujo_agua': 500.0,
                            'flujo_agua_domo_ap': 500.0,
                            'flujo_agua_domo_mp': 500.0,
                            'flujo_agua_recalentador': 300.0,
                            'posicion_valvula_recirc': 50.0,
                            'presion_agua_mp': 30.0,
                            'presion_succion_baa': 5.0,
                            'excentricidad_bomba': 0.05,
                            'temperatura_estator': 70.0,
                            'flujo_salida_12fpmfc': 400.0
                        }
                        
                        # Añadir valores por defecto para los campos faltantes
                        for campo in campos_faltantes:
                            if campo in valores_por_defecto:
                                datos_sensores[campo] = valores_por_defecto[campo]
                                print(f"Usando valor por defecto para '{campo}': {valores_por_defecto[campo]}")
                        
                        # Verificar nuevamente si tenemos todos los campos requeridos
                        campos_faltantes_final = [campo for campo in campos_requeridos if campo not in datos_sensores]
                        
                        if campos_faltantes_final:
                            print(f"Aún faltan campos obligatorios: {campos_faltantes_final}")
                            continue
                        
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
                            print(f"Error al enviar la predicción unificada: {e}")
                            import traceback
                            traceback.print_exc()

                    # Limpieza de conjuntos antiguos incompletos
                    if len(datos_por_tiempo) > 10:
                        print("Limpiando conjuntos de datos antiguos incompletos...")
                        tiempos_ordenados = sorted(datos_por_tiempo.keys())
                        for t in tiempos_ordenados[:-10]:
                            if len(datos_por_tiempo[t]) < len(campos_requeridos):
                                print(f"Eliminando conjunto incompleto para tiempo {t} con {len(datos_por_tiempo[t])}/{len(campos_requeridos)} campos")
                                del datos_por_tiempo[t]

        except Exception as e:
            print(f"Error inesperado: {e}")
            import traceback
            traceback.print_exc()

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
        self.wfile.write(b"<html><body><h1>Listener de Bomba A funcionando</h1><p>El servicio esta activo y escuchando notificaciones PostgreSQL.</p></body></html>")
    
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
