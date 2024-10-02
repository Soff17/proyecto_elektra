# Usar la imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos de requisitos y el código de la aplicación
COPY requirements.txt requirements.txt
COPY . .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto 5001
EXPOSE 5001

# Ejecutar la aplicación en el puerto 5001
CMD ["python", "main.py"]