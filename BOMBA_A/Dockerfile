# Dockerfile para el listener de PostgreSQL
FROM python:3.12-slim

WORKDIR /code

# Copiar solo los requisitos primero para aprovechar la caché de Docker
COPY ./requirements.txt /code/requirements.txt

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copiar el código de la aplicación
COPY ./ /code/

# Definir variables de entorno por defecto
# Estas pueden ser sobreescritas al ejecutar el contenedor
ENV DB_NAME=ibmclouddb \
    DB_USER=ibm_cloud_7156e033_6e6d_4358_80c8_741dec090199 \
    DB_PASSWORD=8kCEghWkKqMJBnohlctNZ5FWc8g2GsxY \
    DB_HOST=e156f952-3855-49d3-90db-44410d01f1fe.c5km1ted03t0e8geevf0.databases.appdomain.cloud \
    DB_PORT=31078 \
    BASE_URL=http://host.docker.internal:8000/sensores \
    BASE_URL_B=http://host.docker.internal:8000/sensores_b

# No es necesario exponer puertos ya que este servicio no escucha conexiones entrantes

# Comando para ejecutar el listener
CMD ["python", "listener.py"]
