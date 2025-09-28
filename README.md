# Documentación de Procesamiento - Cliente

## Descripción General

Este documento describe la implementación de la capa de procesamiento del cliente Go en el sistema analizador de cafeterías. Esta capa maneja el procesamiento de archivos CSV, la gestión de lotes de datos y la implementación de manejadores específicos para diferentes tipos de tablas.

## Estructura del Paquete

La implementación de procesamiento se encuentra en el directorio `client/common/processing/` y consta de los siguientes archivos:

- `batch_processing.go` - Maneja la lógica de procesamiento por lotes y envío de datos
- `file_processing.go` - Gestiona el procesamiento de archivos CSV y directorios
- `handlers.go` - Define manejadores específicos para cada tipo de tabla de datos


# Documentación de Red - Cliente

## Descripción General

Este documento describe la implementación de la capa de red del cliente Go en el sistema analizador de cafeterías. Esta capa maneja las conexiones TCP, el procesamiento de respuestas del servidor y el envío de mensajes de finalización.

## Estructura del Paquete

La implementación de red se encuentra en el directorio `client/common/network/` y consta de los siguientes archivos:

- `connection.go` - Maneja la lógica de conexión TCP con reintentos automáticos
- `response_handling.go` - Procesa las respuestas del servidor y maneja mensajes de finalización

# Documentación de Procesamiento del Orquestador - Orchestrator/common/processing

## Descripción General

Este documento describe la implementación de la capa de procesamiento del orquestador Python en el sistema analizador de cafeterías. Esta capa proporciona un sistema completo de filtrado, serialización y transformación de datos, preparándolos para análisis downstream y transmisión a consumidores especializados.

## Estructura del Paquete

La implementación de procesamiento se encuentra en el directorio `orchestrator/common/processing/` y consta de los siguientes archivos:

- `__init__.py` - Exporta las funciones y clases principales del paquete
- `batch_processor.py` - Orquestación principal del procesamiento de lotes
- `filters.py` - Funciones de filtrado por tipo de datos
- `serialization.py` - Serialización binaria de datos filtrados  
- `file_utils.py` - Utilidades de logging y escritura de archivos para debugging
