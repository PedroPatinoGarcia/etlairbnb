============================================================
Staging Data Documentation
============================================================

.. module:: staging
   :synopsis: Módulo de flujo de datos ocupado de limpieza y optimizacion.


Clase `HandlerBranchStaging`
----------------------------------------------------

.. autoclass:: HandlerBranchStaging
   :members:
   :undoc-members:
   :show-inheritance:

Función Principal `process_latest_raw()`
------------------------------------------------------

.. autofunction:: staging.HandlerBranchStaging.process_latest_raw 
   :no-index:

Detalles Adicionales
------------------------------------------------

### Estructura de Archivos

El proyecto tiene la siguiente estructura de directorios:

- `staging/`: Directorio donde se almacenan los datos modificados en formato Parquet.

### Dependencias

- PySpark: Se requiere para la manipulación y procesamiento de grandes volúmenes de datos.

### Configuración

El archivo `spark_session.py` configura la sesión de Spark para el proyecto.

### Consideraciones de Uso

- Asegúrese de tener los permisos adecuados para crear carpetas y escribir archivos en el sistema de archivos.
