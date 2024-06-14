==============================
Raw Data Documentation
==============================

.. module:: raw
   :synopsis: Módulo que maneja la lectura y escritura de datos en PySpark.


Clase `HandlerBranchCode`
-------------------------

.. autoclass:: HandlerBranchCode
   :members:
   :undoc-members:
   :show-inheritance:

Función Principal `main()`
---------------------------

.. autofunction:: main

Detalles Adicionales
---------------------

### Estructura de Archivos

El proyecto tiene la siguiente estructura de directorios:

- `raw/`: Directorio donde se almacenan los datos originales en formato Parquet.

### Dependencias

- PySpark: Se requiere para la manipulación y procesamiento de grandes volúmenes de datos.

### Configuración

El archivo `spark_session.py` configura la sesión de Spark para el proyecto.

### Consideraciones de Uso

- Asegúrese de tener los permisos adecuados para crear carpetas y escribir archivos en el sistema de archivos.
