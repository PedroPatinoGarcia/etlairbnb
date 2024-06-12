# Usa la imagen base de Ubuntu
FROM ubuntu:latest

# Copia los archivos de tu carpeta al contenedor
COPY modules/ /modules/
COPY script.py /

# Establece el directorio de trabajo
WORKDIR /app

# Ejecuta cualquier comando necesario para configurar tu aplicación
CMD ["python3", "script.py"]


