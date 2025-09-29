# Coffee Shop Aggregator

Este módulo implementa el agregador de datos para el sistema de análisis de cafeterías.

## Estructura

- `__init__.py` - Inicialización del paquete
- `__main__.py` - Punto de entrada principal
- `aggregator.py` - Lógica principal del agregador
- `config.ini` - Archivo de configuración de ejemplo

## Uso

### Ejecutar como módulo

```bash
# Desde el directorio raíz del proyecto
python -m aggregator

# Con parámetros personalizados
python -m aggregator --host 0.0.0.0 --port 9090 --debug

# Con archivo de configuración
python -m aggregator --config aggregator/config.ini
```

### Ejecutar directamente

```bash
cd aggregator
python __main__.py
```

### Importar en código

```python
from aggregator import CoffeeShopAggregator

# Crear una instancia
aggregator = CoffeeShopAggregator(host="localhost", port=8080)

# Iniciar el servicio
aggregator.start()
```

## Parámetros de línea de comandos

- `--host` - Host donde escuchar (default: localhost)
- `--port` - Puerto donde escuchar (default: 8080)
- `--config` - Archivo de configuración
- `--debug` - Habilitar logging debug

## Funcionalidades

- **Servidor multi-cliente**: Acepta múltiples conexiones simultáneas
- **Agregación en tiempo real**: Procesa y almacena datos de clientes
- **Limpieza automática**: Elimina datos antiguos automáticamente
- **Configuración flexible**: Soporta archivos de configuración
- **Logging completo**: Registra todas las actividades
- **API de estadísticas**: Métodos para obtener estadísticas y datos

## Configuración

El archivo `config.ini` permite configurar:

- Número máximo de clientes
- Tamaño del buffer de red
- Intervalo de agregación
- Tiempo de retención de datos
- Configuración de logging