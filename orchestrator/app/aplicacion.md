# Documentación de Aplicación Orquestador - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación de la capa de aplicación del orquestador Python en el sistema analizador de cafeterías. Esta capa proporciona una arquitectura modular que maneja la recepción de mensajes de múltiples clientes, el procesamiento de datos y el filtrado de información.

## Estructura del Paquete

La implementación de la aplicación se encuentra en el directorio `orchestrator/app/` y consta de los siguientes archivos:

- `__init__.py` - Archivo de inicialización del paquete (vacío)
- `net.py` - Implementación principal del orquestador con arquitectura modular

## Arquitectura Modular

El orquestador utiliza una arquitectura modular que separa las responsabilidades en diferentes componentes:

### Componentes Importados

```python
from common.network import ServerManager, MessageHandler, ResponseHandler
from common.processing import create_filtered_data_batch, message_logger
from common.protocol import Opcodes
```

- **common.network**: Gestión de red, manejo de mensajes y respuestas
- **common.processing**: Procesamiento y filtrado de datos, logging de mensajes
- **common.protocol**: Definiciones del protocolo de comunicación

## Clase Principal: Orchestrator

### Estructura
```python
class Orchestrator:
    def __init__(self, port: int, listen_backlog: int)
    def _setup_message_processors(self)
    def _handle_message(self, msg, client_sock) -> bool
    def _process_data_message(self, msg, client_sock) -> bool
    def _process_filtered_batch(self, msg, status_text: str)
    def run(self)
```

### Constructor

```python
def __init__(self, port: int, listen_backlog: int):
```

**Parámetros:**
- `port`: Puerto en el que escucha el servidor
- `listen_backlog`: Número máximo de conexiones pendientes

**Comportamiento:**
- Inicializa el `MessageHandler` para gestionar diferentes tipos de mensajes
- Crea el `ServerManager` con la función de manejo de mensajes
- Configura los procesadores de mensajes específicos

### Funciones Principales

#### `_setup_message_processors()`
Configura los procesadores de mensajes para diferentes tipos de datos.

**Registra procesadores para:**
- `Opcodes.NEW_MENU_ITEMS` - Elementos del menú
- `Opcodes.NEW_STORES` - Información de tiendas
- `Opcodes.NEW_TRANSACTION_ITEMS` - Elementos de transacciones
- `Opcodes.NEW_TRANSACTION` - Transacciones completas
- `Opcodes.NEW_USERS` - Información de usuarios

**Nota:** El mensaje `FINISHED` es manejado por defecto (retorna `False` para cerrar la conexión)

#### `_handle_message(msg, client_sock) -> bool`
Manejador principal de mensajes que delega a procesadores específicos.

**Parámetros:**
- `msg`: Objeto del mensaje recibido
- `client_sock`: Socket del cliente para enviar respuestas

**Retorna:**
- `True`: Continuar la conexión
- `False`: Cerrar la conexión

**Comportamiento:**
- Delega el manejo al `MessageHandler` registrado
- Proporciona una capa de abstracción entre la red y el procesamiento

#### `_process_data_message(msg, client_sock) -> bool`
Procesa mensajes de datos con filtrado y logging detallado.

**Proceso de Procesamiento:**

1. **Obtención del Estado**: Extrae el estado del lote del mensaje
2. **Logging Original** (comentado): Escribe el mensaje original recibido
3. **Procesamiento Filtrado**: Procesa y filtra el lote de datos
4. **Logging de Éxito**: Registra el procesamiento exitoso del lote
5. **Vista Previa**: Registra una vista previa de los datos
6. **Respuesta**: Envía respuesta de éxito al cliente

**Manejo de Errores:**
- Captura cualquier excepción durante el procesamiento
- Delega el manejo de errores al `ResponseHandler`

#### `_process_filtered_batch(msg, status_text: str)`
Procesa y registra el lote filtrado.

**Funcionalidad:**

1. **Creación del Lote Filtrado**:
   ```python
   filtered_batch = create_filtered_data_batch(msg)
   ```

2. **Serialización**: Convierte el lote filtrado a bytes
   ```python
   batch_bytes = filtered_batch.to_bytes()
   ```

3. **Integración Futura**: Preparado para envío a cola de mensajes
   ```python
   # self._filter_router_queue.send(batch_bytes)
   ```

4. **Logging de Archivos** (comentado): Capacidad para escribir mensajes filtrados a archivos

5. **Logging de información**: Registra información del lote filtrado

**Manejo de Errores del Filtro:**
- Captura errores específicos de filtrado
- Registra warnings con detalles del lote y error

#### `run()`
Inicia el servidor del orquestador.

**Comportamiento:**
- Delega la ejecución al `ServerManager`
- Inicia el bucle principal del servidor

## Flujo de Operación

### 1. Inicialización
```python
# Crear orquestador
orchestrator = Orchestrator(port=8080, listen_backlog=10)

# Configurar procesadores (automático)
# - Registra procesadores para cada tipo de datos
# - Configura manejo de mensajes FINISHED
```

### 2. Procesamiento de Mensajes
```python
# Recepción de mensaje
msg = receive_message_from_client()

# Delegación de manejo
continue_connection = orchestrator._handle_message(msg, client_sock)

# Si es mensaje de datos:
# 1. Obtener estado del lote
# 2. Procesar lote filtrado
# 3. Logging detallado
# 4. Enviar respuesta de éxito
```

### 3. Procesamiento de Lote
```python
# Crear lote filtrado
filtered_batch = create_filtered_data_batch(msg)

# Serializar a bytes
batch_bytes = filtered_batch.to_bytes()

# Logging de información
message_logger.log_batch_filtered(filtered_batch, status_text)

# Preparado para cola de mensajes (futuro)
# message_queue.send(batch_bytes)
```

### 4. Ejecución del Servidor
```python
# Iniciar servidor
orchestrator = Orchestrator(8080, 10)
orchestrator.run()

# El ServerManager maneja:
# - Aceptación de conexiones
# - Recepción de mensajes
# - Delegación a procesadores
# - Manejo de errores y cleanup
```

## Características de Diseño

### Arquitectura Modular

1. **Separación de Responsabilidades**:
   - **Network**: Gestión de conexiones y comunicación
   - **Processing**: Filtrado y transformación de datos
   - **Protocol**: Definiciones del protocolo

2. **Inyección de Dependencias**:
   - Los componentes son inyectados en el constructor
   - Facilita testing y modularidad

3. **Patrón Strategy**:
   - Diferentes procesadores para diferentes tipos de mensajes
   - Extensible para nuevos tipos de datos

### Escalabilidad y Performance

1. **Procesamiento Asíncrono**:
   - Preparado para integración con colas de mensajes
   - Separación entre recepción y procesamiento

2. **Logging Configurable**:
   - Logging detallado comentado para debugging
   - Logging de producción optimizado

3. **Filtrado de Datos**:
   - Procesamiento de datos antes de almacenamiento
   - Reducción de carga en componentes downstream

### Manejo de Errores

1. **Captura Granular**:
   - Errores de procesamiento general
   - Errores específicos de filtrado
   - Respuestas apropiadas por tipo de error

2. **Logging Detallado**:
   - Información de contexto en errores
   - Trazabilidad completa del procesamiento

3. **Recuperación Graceful**:
   - Continuación del procesamiento después de errores
   - Respuestas apropiadas a clientes

## Integración con Componentes Externos

### Colas de Mensajes (Futuro)

```python
# Preparado para integración
# self._filter_router_queue.send(batch_bytes)
```

**Beneficios:**
- Desacoplamiento entre recepción y procesamiento
- Escalabilidad horizontal
- Tolerancia a fallos

### Sistema de Archivos (Debugging)

```python
# Capacidades comentadas para debugging
# message_logger.write_original_message(msg, status_text)
# message_logger.write_filtered_message(filtered_batch, status_text)
```

**Uso:**
- Debugging de mensajes grandes
- Análisis de datos filtrados
- Auditoría de procesamiento

## Configuración y Parámetros

### Parámetros del Servidor

- **Puerto**: Puerto de escucha configurable
- **Listen Backlog**: Número de conexiones pendientes
- **Logging Level**: Configurable a través de variables de entorno

### Tipos de Mensaje Soportados

| OpCode | Tipo de Datos | Procesador |
|--------|---------------|------------|
| `NEW_MENU_ITEMS` | Elementos del menú | `_process_data_message` |
| `NEW_STORES` | Tiendas | `_process_data_message` |
| `NEW_TRANSACTION_ITEMS` | Items de transacción | `_process_data_message` |
| `NEW_TRANSACTION` | Transacciones | `_process_data_message` |
| `NEW_USERS` | Usuarios | `_process_data_message` |
| `FINISHED` | Finalización | Manejo por defecto |

## Logging y Monitoreo

### Eventos Registrados

1. **Procesamiento de Lotes**:
   - Éxito/fallo de procesamiento
   - Información de lotes filtrados
   - Vista previa de datos

2. **Errores de Filtrado**:
   - Warnings con contexto completo
   - Número de lote y opcode
   - Detalles del error

3. **Estado de Conexiones**:
   - Continuación/cierre de conexiones
   - Respuestas enviadas a clientes

### Formato de Logs

```python
logging.warning(
    "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
    getattr(msg, 'batch_number', 0), msg.opcode, str(filter_error)
)
```

## Dependencias

### Módulos Internos
- `common.network.ServerManager` - Gestión del servidor
- `common.network.MessageHandler` - Manejo de mensajes
- `common.network.ResponseHandler` - Envío de respuestas
- `common.processing.create_filtered_data_batch` - Filtrado de datos
- `common.processing.message_logger` - Logging de mensajes
- `common.protocol.Opcodes` - Códigos de operación

### Módulos Estándar
- `logging` - Sistema de logging de Python

## Extensibilidad

### Agregar Nuevo Tipo de Mensaje

1. **Definir OpCode** en `common.protocol.Opcodes`
2. **Registrar Procesador**:
   ```python
   self.message_handler.register_processor(
       Opcodes.NEW_MESSAGE_TYPE, 
       self._process_data_message
   )
   ```
3. **Crear Procesador Específico** (opcional):
   ```python
   def _process_custom_message(self, msg, client_sock) -> bool:
       # Lógica específica
       return True
   ```

### Personalizar Procesamiento

1. **Filtros Personalizados**:
   - Modificar `create_filtered_data_batch`
   - Implementar filtros específicos por tipo

2. **Logging Personalizado**:
   - Extender `message_logger`
   - Agregar métricas específicas

3. **Integración de Colas**:
   - Implementar `_filter_router_queue`
   - Configurar sistema de mensajería

## Consideraciones de Rendimiento

### Optimizaciones Actuales

1. **Logging Condicional**: Logging costoso comentado para producción
2. **Procesamiento Streaming**: No almacena mensajes completos en memoria
3. **Delegación Eficiente**: Mínimo overhead en manejo de mensajes

### Optimizaciones Futuras

1. **Colas Asíncronas**: Desacoplamiento de procesamiento
2. **Pooling de Conexiones**: Reutilización de recursos
3. **Caching de Filtros**: Optimización de filtrado repetitivo

## Consideraciones de Seguridad

- **Validación de Mensajes**: Manejo seguro de mensajes malformados
- **Límites de Recursos**: Control de memoria y CPU
- **Logging Seguro**: No exposición de datos sensibles
- **Manejo de Errores**: Prevención de leaks de información

## Patrones de Diseño Utilizados

1. **Strategy Pattern**: Diferentes procesadores por tipo de mensaje
2. **Dependency Injection**: Componentes inyectados en constructor
3. **Template Method**: Flujo de procesamiento estándar con variaciones
4. **Observer Pattern**: Logging de eventos durante procesamiento
5. **Facade Pattern**: Interfaz simplificada para componentes complejos