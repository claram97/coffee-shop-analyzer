# Documentación de Red del Orquestador - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación de la capa de red del orquestador Python en el sistema analizador de cafeterías. Esta capa proporciona una arquitectura modular para la gestión de conexiones TCP, manejo de mensajes entrantes y envío de respuestas estandarizadas a múltiples clientes.

## Estructura del Paquete

La implementación de red se encuentra en el directorio `orchestrator/common/network/` y consta de los siguientes archivos:

- `__init__.py` - Exporta las clases principales del paquete
- `connection.py` - Gestión de conexiones TCP y threads de clientes
- `message_handling.py` - Manejo de mensajes entrantes y respuestas

## Arquitectura del Sistema

### Exportaciones del Paquete (`__init__.py`)

```python
from .connection import ConnectionManager, ServerManager
from .message_handling import MessageHandler, ResponseHandler
```

**Clases Exportadas:**
- `ConnectionManager` - Gestión de bajo nivel de conexiones TCP
- `ServerManager` - Abstracción de alto nivel para el servidor
- `MessageHandler` - Dispatching y procesamiento de mensajes
- `ResponseHandler` - Envío de respuestas estandarizadas

## Componentes Principales

### 1. ConnectionManager (connection.py)

Maneja las tareas de bajo nivel de gestión de conexiones TCP, incluyendo escucha, aceptación y manejo de comunicaciones individuales de clientes en threads separados.

#### Estructura
```python
class ConnectionManager:
    def __init__(self, port: int, listen_backlog: int)
    def set_stop_flag(self)
    def close_server_socket(self)
    def is_stopped(self) -> bool
    def accept_connection(self) -> socket.socket
    def create_client_thread(self, client_sock: socket.socket, message_handler: Callable) -> threading.Thread
    def join_all_threads(self)
    def _handle_client_connection(self, client_sock: socket.socket, message_handler: Callable)
```

#### Constructor

```python
def __init__(self, port: int, listen_backlog: int):
```

**Parámetros:**
- `port`: Número de puerto en el que el servidor escuchará conexiones
- `listen_backlog`: Número máximo de conexiones en cola pendientes de aceptación

**Inicialización:**
- Crea y vincula un socket del servidor TCP
- Configura el socket en modo de escucha
- Inicializa flag de parada y lista de threads

#### Funciones Principales

##### `accept_connection() -> socket.socket`
Bloquea hasta que se recibe una nueva conexión de cliente y la acepta.

**Comportamiento:**
- Registra el proceso de aceptación de conexiones
- Acepta conexión entrante y registra la IP del cliente
- Maneja excepciones OSError durante shutdown graceful

**Retorna:** Socket del cliente conectado

**Excepciones:**
- `OSError`: Si la llamada accept falla y el servidor no está detenido

##### `create_client_thread(client_sock, message_handler) -> threading.Thread`
Crea, inicia y rastrea un nuevo thread para manejar comunicación con un cliente conectado.

**Parámetros:**
- `client_sock`: Socket del cliente conectado
- `message_handler`: Función callable para procesar mensajes del cliente

**Comportamiento:**
- Crea thread con función `_handle_client_connection` como target
- Agrega thread a la lista de threads activos
- Inicia el thread inmediatamente

##### `join_all_threads()`
Espera a que todos los threads activos de manejo de clientes terminen su ejecución.

**Uso:** Típicamente durante shutdown graceful para esperar que todos los clientes terminen.

##### `set_stop_flag()` y `close_server_socket()`
Métodos para iniciación de shutdown graceful.

- `set_stop_flag()`: Señala al servidor que inicie el proceso de shutdown
- `close_server_socket()`: Cierra el socket principal para prevenir nuevas conexiones

#### Función Interna

##### `_handle_client_connection(client_sock, message_handler)`
Función target para thread de manejo de cliente.

**Comportamiento:**
- Entra en bucle para recibir y procesar mensajes continuamente
- Usa `recv_msg()` del protocolo para recibir mensajes enmarcados
- Registra cada mensaje recibido con IP y opcode
- Delega procesamiento al `message_handler`
- Termina conexión si `message_handler` retorna `False`

**Manejo de Errores:**
- `ProtocolError`: Error de protocolo, cierra conexión
- `EOFError`: Cliente cerró conexión gracefully
- `OSError`: Conexión cerrada forzosamente

### 2. ServerManager (connection.py)

Proporciona una abstracción de alto nivel para ejecutar el servidor, integrando gestión de conexiones con shutdown graceful mediante manejo de señales del OS.

#### Estructura
```python
class ServerManager:
    def __init__(self, port: int, listen_backlog: int, message_handler: Callable)
    def setup_signal_handler(self)
    def run(self)
    def _handle_sigterm(self, *_)
```

#### Constructor

```python
def __init__(self, port: int, listen_backlog: int, message_handler: Callable):
```

**Parámetros:**
- `port`: Puerto del servidor
- `listen_backlog`: Conexiones pendientes máximas
- `message_handler`: Función responsable del procesamiento de mensajes de cliente

#### Funciones Principales

##### `run()`
Inicia el bucle principal del servidor.

**Proceso:**
1. Configura manejo de señales (`setup_signal_handler()`)
2. Acepta conexiones continuamente hasta que se inicie shutdown
3. Delega cada conexión a un nuevo thread worker
4. Después del bucle, espera que todos los threads terminen
5. Ejecuta `logging.shutdown()` para flush de logs

##### `setup_signal_handler()`
Configura un manejador de señales para SIGTERM para permitir shutdown graceful del servidor.

##### `_handle_sigterm(*_)`
Función callback ejecutada al recibir señal SIGTERM.

**Acciones:**
- Establece flag de stop en el `ConnectionManager`
- Cierra socket del servidor para desbloquear llamada `accept()`

### 3. MessageHandler (message_handling.py)

Maneja el dispatching de mensajes entrantes a funciones procesadoras registradas basado en su opcode.

#### Estructura
```python
class MessageHandler:
    def __init__(self)
    def register_processor(self, opcode: int, processor_func: Any)
    def get_status_text(self, batch_status: int) -> str
    def is_data_message(self, msg: Any) -> bool
    def handle_message(self, msg: Any, client_sock: Any) -> bool
    def _handle_data_message(self, msg: Any, client_sock: Any) -> bool
    def send_success_response(self, client_sock: Any)
    def send_failure_response(self, client_sock: Any)
    def log_batch_preview(self, msg: Any, status_text: str)
```

#### Funciones Principales

##### `register_processor(opcode: int, processor_func: Any)`
Registra una función procesadora para manejar un opcode específico de mensaje.

**Uso:**
```python
handler = MessageHandler()
handler.register_processor(Opcodes.NEW_MENU_ITEMS, process_menu_items)
```

##### `handle_message(msg: Any, client_sock: Any) -> bool`
Dispatcha un mensaje recibido al procesador apropiado.

**Flujo de Decisión:**
1. **Procesador específico registrado**: Usa procesador personalizado
2. **Mensaje de datos**: Usa `_handle_data_message()` por defecto
3. **Mensaje FINISHED**: Retorna `False` para cerrar conexión
4. **Opcode desconocido**: Registra warning pero mantiene conexión

**Retorna:**
- `True`: Mantener conexión abierta
- `False`: Cerrar conexión

##### `is_data_message(msg: Any) -> bool`
Determina si un mensaje dado transporta datos.

**Criterio:** El opcode no es señal de control (FINISHED) ni mensaje de respuesta.

##### `get_status_text(batch_status: int) -> str`
Convierte código numérico de estado de lote en string legible.

**Mapeo:**
- `0`: "Continue" - Hay más lotes en el archivo
- `1`: "EOF" - Último lote del archivo
- `2`: "Cancel" - Lote enviado por cancelación

##### `log_batch_preview(msg: Any, status_text: str)`
Registra vista previa resumida de un lote de datos para debugging.

**Información incluida:**
- Número de lote y estado
- Opcode del mensaje
- Claves de los campos de datos
- Muestra de las primeras 2 filas

#### Funciones de Respuesta

##### `send_success_response(client_sock)` y `send_failure_response(client_sock)`
Envían respuestas predefinidas de éxito (`BATCH_RECV_SUCCESS`) o fallo (`BATCH_RECV_FAIL`) al cliente.

#### Función de Manejo por Defecto

##### `_handle_data_message(msg: Any, client_sock: Any) -> bool`
Mecanismo de manejo por defecto para mensajes de datos cuando no hay procesador específico registrado.

**Proceso:**
1. Obtiene texto de estado del lote
2. Registra recepción del mensaje de datos
3. Envía respuesta de éxito
4. Maneja errores enviando respuesta de fallo

### 4. ResponseHandler (message_handling.py)

Proporciona métodos estáticos utilitarios para enviar respuestas estandarizadas.

#### Estructura
```python
class ResponseHandler:
    @staticmethod
    def send_success(client_sock: Any)
    @staticmethod
    def send_failure(client_sock: Any)
    @staticmethod
    def handle_processing_error(msg: Any, client_sock: Any, error: Exception) -> bool
```

#### Métodos Estáticos

##### `send_success(client_sock)` y `send_failure(client_sock)`
Envían respuestas `BATCH_RECV_SUCCESS` o `BATCH_RECV_FAIL` al cliente especificado.

##### `handle_processing_error(msg, client_sock, error) -> bool`
Manejador centralizado para logging de errores y notificación al cliente.

**Proceso:**
1. Envía respuesta de fallo al cliente
2. Registra error con detalles relevantes del mensaje
3. Retorna `True` para mantener conexión abierta

**Información de logging:**
- Número de lote que causó el error
- Cantidad de elementos en el lote
- Detalles del error capturado

## Flujo de Operación Completo

### 1. Inicialización del Servidor
```python
# Crear manejador de mensajes
message_handler = MessageHandler()
message_handler.register_processor(Opcodes.NEW_MENU_ITEMS, process_menu_items)

# Crear y configurar servidor
server = ServerManager(port=8080, listen_backlog=10, message_handler.handle_message)
```

### 2. Ejecución del Servidor
```python
# Iniciar servidor (bloquea hasta SIGTERM)
server.run()

# Proceso interno:
# 1. Configura manejo de SIGTERM
# 2. Bucle de aceptación de conexiones
# 3. Crea thread por cliente
# 4. Shutdown graceful al recibir SIGTERM
```

### 3. Manejo de Cliente Individual
```python
# En cada thread de cliente:
while not stopped:
    # 1. Recibir mensaje enmarcado
    msg = recv_msg(client_sock)
    
    # 2. Log de recepción
    log_message_received(msg)
    
    # 3. Procesar mensaje
    continue_connection = message_handler.handle_message(msg, client_sock)
    
    # 4. Cerrar conexión si se indica
    if not continue_connection:
        break
```

### 4. Procesamiento de Mensaje
```python
# En MessageHandler:
# 1. Buscar procesador registrado
if opcode in registered_processors:
    return registered_processors[opcode](msg, client_sock)

# 2. Manejo por defecto para datos
if is_data_message(msg):
    return _handle_data_message(msg, client_sock)

# 3. Manejo de FINISHED
elif opcode == FINISHED:
    return False  # Cerrar conexión
```

## Manejo de Errores y Recuperación

### Tipos de Error

1. **Errores de Protocolo (`ProtocolError`)**
   - Mensaje malformado o opcode inválido
   - **Acción**: Cerrar conexión del cliente problemático

2. **Errores de Conexión (`OSError`)**
   - Conexión cerrada forzosamente por cliente
   - **Acción**: Cleanup y log, continuar con otros clientes

3. **Errores de Procesamiento (`Exception`)**
   - Error en lógica de procesamiento de datos
   - **Acción**: Enviar respuesta de fallo, mantener conexión

4. **Fin de Archivo (`EOFError`)**
   - Cliente cerró conexión gracefully
   - **Acción**: Cleanup normal, sin log de error

### Estrategias de Recuperación

1. **Aislamiento por Thread**: Error en un cliente no afecta otros
2. **Respuestas Estándar**: Cliente recibe feedback sobre errores
3. **Logging Detallado**: Información completa para debugging
4. **Shutdown Graceful**: Manejo de SIGTERM permite cleanup ordenado

## Configuración y Parámetros

### Configuración del Servidor
- **Puerto**: Configurable, típicamente 8080
- **Listen Backlog**: Número de conexiones en cola (típicamente 10-50)
- **Message Handler**: Función de procesamiento inyectada

### Configuración de Logging
- **Nivel de detalle**: INFO para operaciones, DEBUG para vista previa
- **Formato estructurado**: action, result, parámetros específicos
- **Información de contexto**: IP cliente, opcodes, números de lote

## Logging y Monitoreo

### Eventos de Conexión
```python
"action: accept_connections | result: success | ip: {client_ip}"
"action: receive_message | result: success | ip: {ip} | opcode: {opcode}"
```

### Eventos de Procesamiento
```python
"action: data_message_received | opcode: {opcode} | amount: {amount} | status: {status}"
"action: message_processing | result: fail | batch_number: {num} | amount: {amount} | error: {error}"
```

### Vista Previa de Datos (Debug)
```python
"action: batch_preview | batch_number: {num} | status: {status} | opcode: {opcode} | keys: {keys} | sample_count: {count} | sample: {data}"
```

## Dependencias

### Módulos Estándar
- `socket` - Comunicaciones TCP
- `threading` - Manejo concurrente de clientes
- `logging` - Sistema de logging
- `signal` - Manejo de señales del OS
- `typing` - Type hints para mejor código

### Módulos Internos
- `..protocol.recv_msg` - Recepción de mensajes enmarcados
- `..protocol.ProtocolError` - Errores de protocolo
- `..protocol.Opcodes` - Códigos de operación
- `..protocol.BatchRecvSuccess/BatchRecvFail` - Mensajes de respuesta

## Escalabilidad y Performance

### Diseño Concurrente
- **Thread por cliente**: Aislamiento y paralelismo
- **Event-driven shutdown**: Respuesta rápida a SIGTERM
- **Non-blocking accept**: Con timeout implícito via señales

### Optimizaciones
- **Socket reuse**: Configuración automática del SO
- **Minimal overhead**: Delegación directa a procesadores
- **Efficient logging**: Logging estructurado sin overhead excesivo

### Límites y Consideraciones
- **Thread limit**: Limitado por OS y memoria disponible
- **Memory per thread**: Stack space por thread de cliente
- **File descriptors**: Límites del OS para sockets abiertos

## Extensibilidad

### Agregar Nuevo Tipo de Mensaje

1. **Definir OpCode** en `common.protocol.Opcodes`
2. **Crear Procesador**:
   ```python
   def process_new_message_type(msg, client_sock) -> bool:
       # Lógica de procesamiento
       ResponseHandler.send_success(client_sock)
       return True
   ```
3. **Registrar Procesador**:
   ```python
   message_handler.register_processor(Opcodes.NEW_TYPE, process_new_message_type)
   ```

### Personalizar Manejo de Conexiones

1. **Custom Connection Manager**: Extender `ConnectionManager`
2. **Custom Message Handler**: Implementar interface similar
3. **Custom Response Types**: Extender `ResponseHandler`

### Integración con Sistemas Externos

1. **Message Queues**: Enviar mensajes procesados a colas
2. **Databases**: Persistir datos directamente desde procesadores
3. **Monitoring**: Integrar métricas y alertas

## Consideraciones de Seguridad

- **Input Validation**: Validación en capa de protocolo
- **Resource Limits**: Límites implícitos por thread/memory del OS
- **Network Security**: Binding solo a interfaces específicas
- **Error Handling**: Sin leak de información interna en respuestas
- **Logging Security**: Sin exposición de datos sensibles en logs

## Patrones de Diseño Utilizados

1. **Strategy Pattern**: Diferentes procesadores por opcode
2. **Template Method**: Flujo estándar con variaciones
3. **Factory Pattern**: Creación de threads de cliente
4. **Observer Pattern**: Logging de eventos del sistema
5. **Facade Pattern**: `ServerManager` como interfaz simplificada
6. **Singleton Pattern**: `ResponseHandler` como utilidad estática