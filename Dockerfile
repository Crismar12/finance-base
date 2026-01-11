# Usa una imagen base de Python
FROM python:3.11-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia los archivos de requisitos e instala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto en el que Flask correrá
EXPOSE 8080

# Define la variable de entorno para que Flask sepa dónde encontrar la aplicación
ENV FLASK_APP=api_src/api.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080

# Ejecuta la aplicación usando Gunicorn, un servidor WSGI de producción
# Ajusta el número de workers según los recursos de tu servidor
CMD ["gunicorn", "--bind=0.0.0.0:8080", "--timeout", "3600", "--workers", "2", "api_src.api:app"]
