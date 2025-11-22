# Sistema de Monitoreo de Bombas - Listeners PostgreSQL

Sistema de listeners en tiempo real para monitoreo y predicción de fallas en bombas de agua industriales (Bomba A y Bomba B) utilizando notificaciones PostgreSQL (LISTEN/NOTIFY).

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura](#arquitectura)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Instalación y Configuración](#instalación-y-configuración)
- [Uso](#uso)
- [Sistema de Reintentos](#sistema-de-reintentos)
- [Docker](#docker)
- [Despliegue en IBM Cloud Engine](#despliegue-en-ibm-cloud-engine)
- [Monitoreo de Sensores](#monitoreo-de-sensores)
- [Solución de Problemas](#solución-de-problemas)

## Descripción General

Este proyecto implementa dos servicios listener independientes que escuchan notificaciones de PostgreSQL en tiempo real para dos bombas industriales diferentes:

- **BOMBA_A**: Monitorea 20 sensores diferentes (corriente, temperatura, presión, flujo, vibración, etc.)
- **BOMBA_B**: Monitorea 15 sensores específicos de la bomba B

Cuando se insertan o actualizan datos de sensores en la base de datos PostgreSQL, se disparan notificaciones NOTIFY que son capturadas por estos listeners. Los datos se agrupan por timestamp y se envían a un backend de predicción para análisis de anomalías.

## Arquitectura

```
┌─────────────────┐
│   PostgreSQL    │
│   (IBM Cloud)   │
│                 │
│  NOTIFY Events  │
└────────┬────────┘
         │
         ├──────────────────┬──────────────────┐
         │                  │                  │
         v                  v                  v
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Listener       │ │  Listener       │ │   Diagnostico   │
│  BOMBA_A        │ │  BOMBA_B        │ │   Script        │
│  (20 canales)   │ │  (15 canales)   │ │                 │
└────────┬────────┘ └────────┬────────┘ └─────────────────┘
         │                   │
         v                   v
┌─────────────────────────────────────┐
│     Backend de Predicción           │
│  - /predecir-bomba (Bomba A)        │
│  - /predecir-bomba-b (Bomba B)      │
│  - Endpoints individuales           │
└─────────────────────────────────────┘
```

## Estructura del Proyecto

```
Segundo_backend_GM/
│
├── .env                      # Configuración de conexión (NO incluir en git)
├── .env.example              # Plantilla de configuración
├── README.md                 # Este archivo
├── diagnostico_notify.py     # Script de diagnóstico de notificaciones
│
├── BOMBA_A/
│   ├── listener.py           # Listener principal para Bomba A
│   ├── Dockerfile            # Imagen Docker para Bomba A
│   └── requirements.txt      # Dependencias Python
│
└── BOMBA_B/
    ├── listener_bomba_b.py   # Listener principal para Bomba B
    ├── Dockerfile            # Imagen Docker para Bomba B
    └── requirements.txt      # Dependencias Python
```

## Requisitos

### Software
- Python 3.12+
- PostgreSQL 12+ (con soporte para LISTEN/NOTIFY)
- Docker (opcional, para contenedorización)

### Dependencias Python
```
psycopg2-binary==2.9.9
requests==2.31.0
python-dotenv==1.0.0
```

## Instalación y Configuración

### 1. Clonar el repositorio
```bash
git clone <repository-url>
cd Segundo_backend_GM
```

### 2. Configurar variables de entorno
```bash
# Copiar el archivo de ejemplo
cp .env.example .env

# Editar .env con tus credenciales reales
nano .env
```

**Contenido del .env:**
```env
DB_NAME='ibmclouddb'
DB_USER='tu_usuario_ibm_cloud'
DB_PASSWORD='tu_password'
DB_HOST='tu-host.databases.appdomain.cloud'
DB_PORT=30226

BASE_URL='http://127.0.0.1:8000/sensores'
BASE_URL_B='http://127.0.0.1:8000/sensores_b'
```

### 3. Instalar dependencias
```bash
# Para Bomba A
cd BOMBA_A
pip install -r requirements.txt

# Para Bomba B
cd ../BOMBA_B
pip install -r requirements.txt
```

## Uso

### Ejecutar localmente

**Bomba A:**
```bash
cd BOMBA_A
python listener.py
```

**Bomba B:**
```bash
cd BOMBA_B
python listener_bomba_b.py
```

**Script de diagnóstico:**
```bash
python diagnostico_notify.py
```

### Verificar estado
Ambos listeners exponen un servidor HTTP en el puerto 8080:
```bash
curl http://localhost:8080
```

## Sistema de Reintentos

El sistema implementa un mecanismo robusto de reintentos con backoff exponencial:

### Características

1. **Dos tipos de contadores:**
   - `intentos_conexion`: Reintentos en la sesión actual
   - `reconexiones_consecutivas`: Contador acumulativo de fallos

2. **Límites configurables:**
   - `MAX_INTENTOS_INICIAL`: 10 intentos en conexión inicial
   - `MAX_RECONEXIONES`: 50 reconexiones consecutivas máximas
   - `TIEMPO_ESPERA_BASE`: 5 segundos base para backoff

3. **Backoff exponencial:**
   ```
   Intento 1: 5 segundos
   Intento 2: 10 segundos
   Intento 3: 20 segundos
   Intento 4: 40 segundos
   Intento 5+: 60 segundos (máximo)
   ```

4. **Detección de fallos:**
   - Heartbeat cada 10 segundos (SELECT 1)
   - Reconexión automática al perder conexión
   - Manejo de excepciones con traceback completo

### Configuración de reintentos

Puedes modificar los límites mediante variables de entorno:

```bash
# En .env o al ejecutar el contenedor
MAX_INTENTOS_INICIAL=15
MAX_RECONEXIONES=100
TIEMPO_ESPERA_BASE=10
```

## Docker

### Construir imágenes

**Bomba A:**
```bash
cd BOMBA_A
docker build -t listener-bomba-a:latest .
```

**Bomba B:**
```bash
cd BOMBA_B
docker build -t listener-bomba-b:latest .
```

### Ejecutar contenedores

**Bomba A:**
```bash
docker run -d \
  --name listener-bomba-a \
  --env-file ../.env \
  -p 8080:8080 \
  listener-bomba-a:latest
```

**Bomba B:**
```bash
docker run -d \
  --name listener-bomba-b \
  --env-file ../.env \
  -p 8081:8080 \
  listener-bomba-b:latest
```

### Health Checks

Los contenedores incluyen health checks automáticos:
```bash
# Verificar estado de salud
docker ps
docker inspect listener-bomba-a | grep Health -A 10
```

## Despliegue en IBM Cloud Engine

### 1. Preparar variables de entorno
```bash
ibmcloud ce secret create --name db-credentials \
  --from-env-file .env
```

### 2. Desplegar aplicaciones

**Bomba A:**
```bash
ibmcloud ce app create \
  --name listener-bomba-a \
  --image <tu-registry>/listener-bomba-a:latest \
  --env-from-secret db-credentials \
  --port 8080 \
  --min-scale 1 \
  --max-scale 2
```

**Bomba B:**
```bash
ibmcloud ce app create \
  --name listener-bomba-b \
  --image <tu-registry>/listener-bomba-b:latest \
  --env-from-secret db-credentials \
  --port 8080 \
  --min-scale 1 \
  --max-scale 2
```

## Monitoreo de Sensores

### Bomba A - 20 Canales

| Canal PostgreSQL | Campo del Modelo | Descripción |
|------------------|------------------|-------------|
| `canal_sensores_corriente` | `corriente_motor` | Corriente del motor |
| `canal_flujo_salida_12fpmfc` | `flujo_salida_12fpmfc` | Flujo de salida |
| `canal_temperatura_estator` | `temperatura_estator` | Temperatura del estator |
| `canal_presion_succion_baa` | `presion_succion_baa` | Presión de succión |
| `canal_presion_agua_mp` | `presion_agua_mp` | Presión de agua MP |
| `canal_posicion_valvula_recirc` | `posicion_valvula_recirc` | Posición válvula |
| `canal_voltaje_barra` | `voltaje_barra` | Voltaje de barra |
| `canal_vibracion_axial_descanso` | `vibracion_axial` | Vibración axial |
| `canal_temperatura_descanso_interno_bomba_1a` | `temp_bomba` | Temperatura bomba |
| `canal_temperatura_descanso_interna_motor_bomba_1a` | `temp_motor` | Temperatura motor |
| ... | ... | ... |

### Bomba B - 15 Canales

| Canal PostgreSQL | Campo del Modelo | Descripción |
|------------------|------------------|-------------|
| `canal_sensores_corriente_b` | `corriente_motor` | Corriente del motor |
| `canal_excentricidad_bomba_b` | `excentricidad_bomba` | Excentricidad |
| `canal_flujo_descarga_b` | `flujo_descarga_ap` | Flujo de descarga |
| `canal_voltaje_barra_b` | `voltaje_barra` | Voltaje de barra |
| ... | ... | ... |

### Agrupación de Datos

Los listeners agrupan datos por `tiempo_sensor` (timestamp):
1. Reciben notificaciones de múltiples sensores
2. Almacenan valores en diccionario temporal
3. Cuando tienen suficientes datos (15-20 campos), envían a predicción
4. Limpian automáticamente datos antiguos incompletos

### Umbral de Envío

- **Bomba A**: Envía con ≥15/20 campos (75%)
- **Bomba B**: Envía con 15/15 campos (100%)

## Solución de Problemas

### Problema: No se reciben notificaciones

**Verificar:**
```sql
-- En PostgreSQL
SELECT * FROM pg_listening_channels();
NOTIFY canal_sensores_corriente, '{"id_sensor": 1, "valor": 100.0, "tiempo_sensor": "2025-01-01T10:00:00"}';
```

### Problema: Conexión se pierde frecuentemente

**Solución:**
- Aumentar `MAX_RECONEXIONES` en variables de entorno
- Verificar estabilidad de red
- Revisar logs: `docker logs listener-bomba-a`

### Problema: Datos no llegan al backend

**Verificar:**
1. URL del backend en `.env`
2. Logs del listener
3. Conectividad: `curl $BASE_URL`

### Ver logs en tiempo real

```bash
# Docker
docker logs -f listener-bomba-a

# Local
tail -f logs/bomba_a.log
```

## Seguridad

- **Usuario no-root**: Los contenedores ejecutan con usuario `listener` (UID 1000)
- **Secrets**: Usar IBM Cloud Engine secrets para credenciales
- **Network**: Configurar firewalls para restringir acceso a PostgreSQL

## Contribuir

1. Fork el repositorio
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## Licencia

[Especificar licencia]

## Contacto

[Información de contacto del equipo]

---

Desarrollado para monitoreo industrial de bombas con análisis predictivo de fallas.
