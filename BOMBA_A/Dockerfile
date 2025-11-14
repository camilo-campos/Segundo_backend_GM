# Dockerfile para el listener de PostgreSQL - Bomba A
FROM python:3.12-slim

WORKDIR /code

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar solo los requisitos primero para aprovechar la caché de Docker
COPY ./requirements.txt /code/requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Crear usuario no-root para ejecutar la aplicación
RUN useradd -m -u 1000 listener && chown -R listener:listener /code

# Copiar el código de la aplicación
COPY --chown=listener:listener ./ /code/

# Variables de entorno por defecto para configuración de reintentos
ENV PORT=8080 \
    MAX_INTENTOS_INICIAL=10 \
    MAX_RECONEXIONES=50 \
    TIEMPO_ESPERA_BASE=5 \
    PYTHONUNBUFFERED=1

# Cambiar a usuario no-root
USER listener

# Exponer el puerto 8080 para IBM Cloud Engine
EXPOSE 8080

# Health check para verificar que el servidor HTTP responde
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Usar exec form para mejor manejo de señales
CMD ["python", "-u", "listener.py"]
