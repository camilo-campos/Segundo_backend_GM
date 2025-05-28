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


# No es necesario exponer puertos ya que este servicio no escucha conexiones entrantes

# Comando para ejecutar el listener
CMD ["python", "listener.py"]
