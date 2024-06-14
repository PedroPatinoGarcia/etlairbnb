========================================================================================================================
Script Documentation
========================================================================================================================

.. module:: script
   :synopsis: Módulo de flujo de datos ocupado de logica de negocio.


Clase `run_script`
--------------------------------------------------------------------------------------------------------

.. autoclass:: run_script
   :members:
   :undoc-members:
   :show-inheritance:

Función Principal `run_script()`
----------------------------------------------------------------------------------------------------------

.. autofunction:: script.run_script
       :noindex:

Detalles Adicionales
----------------------------------------------------------------------------------------------------

### Estructura de Archivos

El proyecto tiene la siguiente estructura de directorios:

- `script/`: Directorio donde se almacenan los datos modificados en formato Parquet.

### Dependencias

- PySpark: Se requiere para la manipulación y procesamiento de grandes volúmenes de datos.

### Configuración

El archivo `spark_session.py` configura la sesión de Spark para el proyecto.

### Consideraciones de Uso

- Asegúrese de tener los permisos adecuados para crear carpetas y escribir archivos en el sistema de archivos.