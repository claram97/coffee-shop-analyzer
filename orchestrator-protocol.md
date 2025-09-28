# Documentación de Protocolo del Orquestador - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación completa del protocolo de comunicación del orquestador Python en el sistema analizador de cafeterías. Este protocolo define el formato binario, los tipos de mensaje, la serialización/deserialización y el dispatching de mensajes entre clientes y el servidor orquestador.

## Estructura del Paquete

La implementación del protocolo se encuentra en el directorio `orchestrator/common/protocol/` y consta de los siguientes archivos:

- `__init__.py` - Exporta todas las clases y funciones principales del protocolo
- `constants.py` - Define constantes, códigos de operación y excepciones
- `entities.py` - Clases de datos en bruto (DTOs) para cada tipo de entidad
- `messages.py` - Clases de mensaje para comunicación bidireccional
- `databatch.py` - Mensaje contenedor para datos con metadata de routing
- `dispatcher.py` - Función principal de recepción y dispatching de mensajes
- `parsing.py` - Funciones de bajo nivel para parsing y escritura binaria

## Arquitectura del Sistema

### Exportaciones del Paquete (`__init__.py`)

```python
# Constantes y excepciones
'ProtocolError', 'Opcodes', 'BatchStatus', 'MAX_BATCH_SIZE_BYTES',

# Entidades de datos
'RawMenuItems', 'RawStore', 'RawTransactionItem', 'RawTransaction', 'RawUser',

# Clases de mensaje
'TableMessage', 'NewMenuItems', 'NewStores', 'NewTransactionItems', 
'NewTransactions', 'NewUsers', 'EOFMessage', 'Finished', 'BatchRecvSuccess', 'BatchRecvFail',

# Mensajes complejos
'DataBatch',

# Manejo de mensajes
'recv_msg',

# Parsing de bajo nivel
'recv_exactly', 'read_u8', 'read_u16', 'read_i32', 'read_i64', 'read_string',
'write_u8', 'write_u16', 'write_i32', 'write_i64', 'write_string'
```

## Componentes Principales

### 1. Constantes y Definiciones (constants.py)

Define las constantes fundamentales, excepciones personalizadas y enumeraciones para el protocolo.

#### Constantes Globales

##### `MAX_BATCH_SIZE_BYTES = 1024 * 1024` (1MB)
Tamaño máximo para el payload de un solo mensaje batch. Actúa como salvaguarda contra mensajes excesivamente grandes.

#### Excepciones

##### `ProtocolError(Exception)`
Excepción personalizada para errores de enmarcado o validación durante el parsing o escritura de mensajes del protocolo.

**Atributos:**
- `opcode` (int, opcional): Identifica el contexto del mensaje donde ocurrió el error

#### Códigos de Operación (Opcodes)

##### `class Opcodes:`
Define los códigos de operación del protocolo para identificar el tipo de cada mensaje.

| OpCode | Valor | Dirección | Descripción |
|--------|-------|-----------|-------------|
| `DATA_BATCH` | 0 | Cualquiera | Mensaje wrapper que contiene otro mensaje específico |
| `BATCH_RECV_SUCCESS` | 1 | Servidor→Cliente | ACK exitoso de procesamiento de lote |
| `BATCH_RECV_FAIL` | 2 | Servidor→Cliente | Notificación de fallo en procesamiento de lote |
| `FINISHED` | 3 | Cliente→Servidor | Señal de control: el emisor terminó de transmitir |
| `NEW_MENU_ITEMS` | 4 | Cliente→Servidor | Lote de nuevos elementos del menú |
| `NEW_STORES` | 5 | Cliente→Servidor | Lote de nuevas tiendas |
| `NEW_TRANSACTION_ITEMS` | 6 | Cliente→Servidor | Lote de nuevos elementos de transacciones |
| `NEW_TRANSACTION` | 7 | Cliente→Servidor | Lote de nuevas transacciones |
| `NEW_USERS` | 8 | Cliente→Servidor | Lote de nuevos usuarios |
| `EOF` | 9 | Cliente→Servidor | **Fin de tabla específica** - Indica que terminó un tipo de datos completo |

#### Estados de Lote (BatchStatus)

##### `class BatchStatus:`
Define códigos de estado incluidos en mensajes de lote de datos para indicar el estado del stream.

| Estado | Valor | Descripción |
|--------|-------|-------------|
| `CONTINUE` | 0 | Más lotes de datos seguirán a este |
| `EOF` | 1 | **Lote final en el stream** - Usado en dos contextos: (1) Último batch de archivo CSV, (2) Estado en mensaje `EOFMessage` |
| `CANCEL` | 2 | Este lote fue enviado como parte de un proceso de cancelación |

**Nota importante sobre EOF:** El estado `EOF` (1) tiene **doble significado**:
1. **A nivel archivo**: Último batch de un archivo CSV individual
2. **A nivel tabla**: Estado del mensaje `EOFMessage` indicando fin de tabla completa (ej: todos los archivos de "menu_items/")

### 2. Entidades de Datos (entities.py)

Define clases de datos que representan la estructura en bruto, sin procesar, de los registros recibidos del cliente. Actúan como Data Transfer Objects (DTOs).

#### `RawMenuItems`
Representa un registro de elemento del menú sin procesar.

**Campos:**
- `product_id: str` - Identificador único del producto
- `name: str` - Nombre del elemento del menú
- `price: str` - Precio del elemento
- `category: str` - Categoría a la que pertenece el elemento
- `is_seasonal: str` - Flag indicando si el elemento es estacional
- `available_from: str` - Fecha desde la que el elemento está disponible
- `available_to: str` - Fecha hasta la que el elemento está disponible

#### `RawStore`
Representa un registro de tienda sin procesar.

**Campos:**
- `store_id: str` - Identificador único de la tienda
- `store_name: str` - Nombre de la tienda
- `street: str` - Dirección de la tienda
- `postal_code: str` - Código postal de la ubicación
- `city: str` - Ciudad donde se encuentra la tienda
- `state: str` - Estado o provincia donde se encuentra la tienda
- `latitude: str` - Latitud geográfica de la tienda
- `longitude: str` - Longitud geográfica de la tienda

#### `RawTransactionItem`
Representa un elemento línea individual dentro de una transacción.

**Campos:**
- `transaction_id: str` - Identificador de la transacción padre
- `item_id: str` - Identificador único del producto/elemento vendido
- `quantity: str` - Número de unidades del elemento vendido
- `unit_price: str` - Precio de una sola unidad del elemento
- `subtotal: str` - Precio total para esta línea de elemento
- `created_at: str` - Timestamp cuando se registró el elemento de transacción

#### `RawTransaction`
Representa un registro de cabecera de transacción sin procesar.

**Campos:**
- `transaction_id: str` - Identificador único de la transacción
- `store_id: str` - Identificador de la tienda donde ocurrió la transacción
- `payment_method_id: str` - Identificador del método de pago usado
- `user_id: str` - Identificador del usuario que hizo la compra
- `original_amount: str` - Monto total antes de descuentos
- `discount_applied: str` - Monto de descuento aplicado
- `final_amount: str` - Monto final pagado después de descuentos
- `created_at: str` - Timestamp cuando se creó la transacción

#### `RawUser`
Representa un registro de usuario sin procesar.

**Campos:**
- `user_id: str` - Identificador único del usuario
- `gender: str` - Género del usuario
- `birthdate: str` - Fecha de nacimiento del usuario
- `registered_at: str` - Timestamp cuando el usuario se registró

### 3. Clases de Mensaje (messages.py)

Define las clases de mensaje para manejo de diferentes tipos de comunicación.

#### `TableMessage` (Clase Base)
Clase base para leer mensajes que contienen datos tabulares. Una tabla es una lista de registros, donde cada registro es un mapa clave-valor.

##### Estructura
```python
class TableMessage:
    def __init__(self, opcode: int, required_keys: Tuple[str, ...], row_factory)
    def _validate_pair_count(self, n_pairs: int)
    def _read_key_value_pairs(self, sock: socket.socket, remaining: int, n_pairs: int)
    def _validate_required_keys(self, current_row_data: dict[str, str])
    def _create_row_object(self, current_row_data: dict[str, str])
    def _read_row(self, sock: socket.socket, remaining: int) -> int
    def _read_table_header(self, sock: socket.socket, remaining: int) -> int
    def _read_all_rows(self, sock: socket.socket, remaining: int, n_rows: int) -> int
    def read_from(self, sock: socket.socket, length: int)
```

##### Constructor
```python
def __init__(self, opcode: int, required_keys: Tuple[str, ...], row_factory):
```

**Parámetros:**
- `opcode`: El opcode del mensaje
- `required_keys`: Tupla de nombres de columna requeridos
- `row_factory`: Función/clase para crear objetos row desde dict

**Atributos inicializados:**
- `rows: List` - Almacena objetos creados (ej: RawMenuItems, RawStore)
- `amount: int` - Número de filas en el lote
- `batch_number: int` - Número de lote del cliente
- `batch_status: int` - Estado del lote (Continue/EOF/Cancel)

##### Proceso de Parsing

**Formato del mensaje del cliente:**
```
[length:i32][nRows:i32][batchNumber:i64][status:u8][rows...]
```

**Flujo de `read_from()`:**
1. **Leer cabecera de tabla**: número de filas, número de lote y estado
2. **Leer todas las filas**: Para cada fila, leer pares clave-valor
3. **Validar longitud**: No deben quedar bytes sin procesar

**Validaciones:**
- Número de pares coincide con claves requeridas
- Todas las claves requeridas están presentes
- Longitud del mensaje coincide exactamente

#### Mensajes Específicos de Tabla

##### `NewMenuItems(TableMessage)`
**Claves requeridas:** `("product_id", "name", "category", "price", "is_seasonal", "available_from", "available_to")`
**Row factory:** `RawMenuItems`

##### `NewStores(TableMessage)`
**Claves requeridas:** `("store_id", "store_name", "street", "postal_code", "city", "state", "latitude", "longitude")`
**Row factory:** `RawStore`

##### `NewTransactionItems(TableMessage)`
**Claves requeridas:** `("transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at")`
**Row factory:** `RawTransactionItem`

##### `NewTransactions(TableMessage)`
**Claves requeridas:** `("transaction_id", "store_id", "payment_method_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at")`
**Row factory:** `RawTransaction`

##### `NewUsers(TableMessage)`
**Claves requeridas:** `("user_id", "gender", "birthdate", "registered_at")`
**Row factory:** `RawUser`

##### `EOFMessage(TableMessage)` **NUEVO**
**Propósito:** Mensaje de fin de tabla específica que indica que el cliente terminó de enviar todos los archivos de un tipo de datos.

**Claves requeridas:** `("table_type",)`
**Row factory:** `lambda **kwargs: None` - No crea objetos row, solo extrae metadatos

**Características especiales:**
- **nRows:** Siempre 1 (una fila virtual con metadatos)
- **batch_status:** Siempre `BatchStatus.EOF` (1)
- **table_type:** String que especifica qué tabla terminó (ej: "menu_items", "stores", "transactions")

**Ejemplo de uso:**
```python
# El cliente envía automáticamente después de procesar:
# menu_items/file1.csv → batches 1-5
# menu_items/file2.csv → batches 6-8  
# menu_items/file3.csv → batches 9-12
# → EOF automático con table_type="menu_items"

# Procesamiento en el servidor:
eof_msg = recv_msg(socket)  # EOFMessage
table_type = eof_msg.get_table_type()  # "menu_items"
logging.info(f"EOF received for table: {table_type}")
```

**Métodos específicos:**
- `create_eof_message(batch_number, table_type)` - Crea mensaje EOF
- `get_table_type() -> str` - Obtiene el tipo de tabla
- `to_bytes() -> bytes` - Serializa para reenvío a colas

#### Mensajes de Control

##### `Finished`
Mensaje entrante FINISHED con un solo agency_id (i32 LE) en el cuerpo.

**Atributos:**
- `opcode = Opcodes.FINISHED`
- `agency_id: int` - ID de la agencia que terminó

##### `BatchRecvSuccess` y `BatchRecvFail`
Respuestas salientes de acknowledgment (cuerpo vacío).

**Métodos:**
- `write_to(sock)`: Envía `[opcode][length=0]`

### 4. DataBatch (databatch.py)

Mensaje contenedor que transporta mensajes específicos de tabla junto con metadata esencial para routing y procesamiento.

#### Estructura
```python
class DataBatch:
    def __init__(self, *, table_ids=None, query_ids=None, meta=None, 
                 total_shards=None, shard_num=None, reserved_u16=0, batch_bytes=None)
    def read_from(self, sock: socket.socket, length: int)
    def write_to(self, sock: socket.socket)
    def to_bytes(self) -> bytes
    @staticmethod
    def make_embedded(inner_opcode: int, inner_body: bytes) -> bytes
```

#### Formato Binario

```
[u8 opcode]         - Siempre DATA_BATCH (0)
[i32 length]        - Longitud del cuerpo siguiente en bytes
[-- CUERPO INICIA --]
  [u8 list table_ids] - Lista de IDs de tabla para los que estos datos pueden ser relevantes
  [u8 list query_ids] - Lista de IDs de query para los que estos datos son relevantes
  [u16 reserved]      - Reservado para flags futuros
  [i64 batch_number]  - El número de lote del mensaje fuente original
  [dict<u8,u8> meta]  - Diccionario para metadata arbitraria
  [u16 total_shards]  - El número total de shards para estos datos
  [u16 shard_num]     - El número de este shard específico
  [embedded message]  - El mensaje interno, enmarcado como [u8 opcode][i32 len][body]
[-- CUERPO TERMINA --]
```

#### Funciones Principales

##### `make_embedded(inner_opcode: int, inner_body: bytes) -> bytes` (estático)
Utilidad para enmarcar un cuerpo de mensaje interno con su opcode y longitud.

**Formato:** `[u8 opcode][i32 length][body]`

##### `read_from(sock: socket.socket, length: int)`
Deserializa un mensaje DataBatch desde un socket, poblando los campos de la instancia.

**Proceso:**
1. Leer cabecera específica de DataBatch
2. Leer mensaje embebido
3. Validar que no queden bytes

##### `to_bytes() -> bytes`
Serializa toda la instancia DataBatch en un solo objeto bytes.

**Requiere:** `self.batch_bytes` debe estar configurado con el mensaje embebido pre-enmarcado

#### Validaciones

- `table_ids` y `query_ids`: Máximo 255 elementos, valores 0-255
- `meta`: Máximo 255 entradas, claves y valores u8
- `total_shards` y `shard_num`: Valores u16 (0-65535)
- `batch_bytes`: Debe tener enmarcado válido del mensaje interno

### 5. Dispatcher (dispatcher.py)

Proporciona la función principal de dispatcher para recibir y rutear todos los mensajes entrantes del protocolo basado en su opcode.

#### `recv_msg(sock: socket.socket)`
Función principal que lee un mensaje completo desde un socket, maneja el enmarcado inicial y despacha el parsing a la clase de mensaje apropiada.

**Proceso:**
1. **Leer cabecera común**: `[u8 opcode][i32 length]`
2. **Validar longitud**: Debe ser >= 0
3. **Instanciar clase apropiada** basado en opcode
4. **Delegar parsing** del cuerpo del mensaje al método `read_from()` de la instancia

**Retorna:** Objeto de mensaje completamente parseado correspondiente al opcode recibido

**Excepciones:**
- `ProtocolError`: Longitud inválida (< 0) o opcode desconocido
- `EOFError`: Socket cerrado inesperadamente durante lectura de cabecera

**Factory de Mensajes:**
```python
if opcode == Opcodes.FINISHED: return Finished()
elif opcode == Opcodes.NEW_MENU_ITEMS: return NewMenuItems()
elif opcode == Opcodes.NEW_STORES: return NewStores()
elif opcode == Opcodes.NEW_TRANSACTION_ITEMS: return NewTransactionItems()
elif opcode == Opcodes.NEW_TRANSACTION: return NewTransactions()
elif opcode == Opcodes.NEW_USERS: return NewUsers()
elif opcode == Opcodes.EOF: return EOFMessage()
elif opcode == Opcodes.DATA_BATCH: return DataBatch()
```

#### `instantiate_message_for_opcode(opcode: int)`
Función factory que crea una instancia de mensaje vacía basada en un opcode para deserialización de mensajes embebidos.

### 6. Funciones de Parsing (parsing.py)

Proporciona funciones utilitarias de bajo nivel y fundacionales para parsing y escritura de tipos de datos primitivos del protocolo binario.

#### Funciones de Lectura de Socket

##### `recv_exactly(sock: socket.socket, n: int) -> bytes`
Lee exactamente `n` bytes de un socket, manejando lecturas cortas mediante reintentos.

**Características:**
- Reemplazo robusto para `sock.recv(n)` simple
- Asegura que se reciba la cantidad completa de datos solicitada
- Manejo de `InterruptedError` y timeouts

**Excepciones:**
- `EOFError`: Si el peer cierra la conexión antes de `n` bytes
- `ProtocolError`: En timeouts, errores de SO, o si `n` es negativo

#### Funciones de Lectura de Tipos de Datos

##### `read_u8(sock: socket.socket) -> int`
Lee un byte sin signo (u8) del socket.

##### `read_u16(sock, remaining, opcode) -> Tuple[int, int]`
Lee un entero sin signo de 2 bytes, little-endian (u16) del socket.

##### `read_i32(sock, remaining, opcode) -> Tuple[int, int]`
Lee un entero con signo de 4 bytes, little-endian (i32) del socket.

##### `read_i64(sock, remaining, opcode) -> Tuple[int, int]`
Lee un entero con signo de 8 bytes, little-endian (i64) del socket.

##### `read_string(sock, remaining, opcode) -> Tuple[str, int]`
Lee una string con prefijo de longitud del socket.

**Formato:** 4-byte signed integer (length) seguido de bytes UTF-8

**Validaciones:**
- Longitud >= 0
- Suficientes bytes restantes
- UTF-8 válido

#### Funciones de Escritura

##### `write_u8(sock: socket.socket, value: int)`
Escribe un solo byte sin signo (u8) al socket.

##### `write_u16(sock: socket.socket, value: int)`
Escribe un entero sin signo de 2 bytes, little-endian (u16) al socket.

##### `write_i32(sock: socket.socket, value: int)`
Escribe un entero con signo de 4 bytes, little-endian (i32) al socket.

##### `write_i64(sock: socket.socket, value: int)`
Escribe un entero con signo de 8 bytes, little-endian (i64) al socket.

##### `write_string(sock: socket.socket, s: str)`
Escribe una string con prefijo de longitud al socket.

#### Funciones Especializadas

##### `read_u8_with_remaining(sock, remaining, opcode) -> Tuple[int, int]`
Lee un u8 y decrementa el contador de bytes restantes.

##### `read_u8_list(sock, remaining, opcode) -> Tuple[List[int], int]`
Lee una lista de valores u8 con prefijo de conteo u8.
**Formato:** `[u8 count][u8 item 1][u8 item 2]...`

##### `read_u8_u8_dict(sock, remaining, opcode) -> Tuple[Dict[int, int], int]`
Lee un diccionario de mapeos u8 -> u8 con prefijo de conteo u8.
**Formato:** `[u8 count][u8 key 1][u8 value 1][u8 key 2][u8 value 2]...`

## Flujo de Operación Completo

### 1. Recepción de Mensaje
```python
# Uso principal
from orchestrator.common.protocol import recv_msg

# En loop del servidor
while True:
    msg = recv_msg(client_socket)
    # msg es instancia específica del tipo (NewMenuItems, DataBatch, etc.)
    
    # Procesar basado en tipo
    if isinstance(msg, NewMenuItems):
        process_menu_items(msg)
    elif isinstance(msg, DataBatch):
        process_data_batch(msg)
    elif isinstance(msg, EOFMessage):
        process_eof_message(msg)
```

### 2. Creación de DataBatch
```python
# Crear mensaje embebido
inner_msg = NewMenuItems()
# ... poblar inner_msg ...
inner_body = serialize_message(inner_msg)

# Crear frame embebido
batch_bytes = DataBatch.make_embedded(Opcodes.NEW_MENU_ITEMS, inner_body)

# Crear DataBatch wrapper
data_batch = DataBatch(
    table_ids=[1, 2],
    query_ids=[1, 2, 3],
    total_shards=1,
    shard_num=0,
    batch_bytes=batch_bytes
)

# Serializar y enviar
data_batch.write_to(socket)
```

### 3. Procesamiento de Mensaje de Tabla
```python
# Recibir mensaje (automático por recv_msg)
menu_msg = recv_msg(socket)  # Retorna NewMenuItems()

# Acceder a datos
print(f"Recibido {menu_msg.amount} items del menú")
print(f"Número de lote: {menu_msg.batch_number}")
print(f"Estado: {menu_msg.batch_status}")

# Procesar filas
for item in menu_msg.rows:  # Lista de objetos RawMenuItems
    print(f"Producto: {item.name}, Precio: {item.price}")
```

### 4. **NUEVO:** Procesamiento de Mensaje EOF
```python
# Recibir mensaje EOF (automático por recv_msg)
eof_msg = recv_msg(socket)  # Retorna EOFMessage()

# Acceder a metadatos
table_type = eof_msg.get_table_type()
print(f"EOF recibido para tabla: {table_type}")
print(f"Número de lote final: {eof_msg.batch_number}")
print(f"Estado: {eof_msg.batch_status}")  # Siempre BatchStatus.EOF (1)

# Procesamiento típico de EOF
def process_eof_message(eof_msg, client_sock):
    table_type = eof_msg.get_table_type()
    
    # Logging del evento
    logging.info(f"action: eof_received | table_type: {table_type}")
    
    # Reenvío a cola downstream (opción A: directo)
    message_bytes = eof_msg.to_bytes()
    filter_router_queue.send(message_bytes)
    
    # O reenvío como DataBatch (opción B: con metadata)
    # databatch = create_eof_databatch(eof_msg, table_type)
    # filter_router_queue.send(databatch)
    
    # Responder al cliente
    BatchRecvSuccess().write_to(client_sock)
    
    logging.info(f"action: eof_processed | table_type: {table_type}")
    return True
```

### 5. Envío de Respuestas
```python
from orchestrator.common.protocol import BatchRecvSuccess, BatchRecvFail

# Enviar éxito
success_response = BatchRecvSuccess()
success_response.write_to(client_socket)

# Enviar fallo  
fail_response = BatchRecvFail()
fail_response.write_to(client_socket)
```

## Formato del Protocolo

### Estructura General de Mensaje
```
[u8 opcode][i32 length][message_body:length_bytes]
```

### Mensajes de Tabla
```
[opcode:1][length:4][nRows:4][batchNumber:8][status:1][rows...]

Cada fila:
[nPairs:4][key1:string][value1:string][key2:string][value2:string]...

String format:
[length:4][utf8_bytes:length]
```

### DataBatch
```
[opcode:1=0][length:4]
[table_ids:u8_list][query_ids:u8_list][reserved:2]
[batch_number:8][meta:u8_u8_dict]
[total_shards:2][shard_num:2]
[embedded_message]
```

### Listas y Diccionarios
```
u8_list: [count:1][item1:1][item2:1]...
u8_u8_dict: [count:1][key1:1][value1:1][key2:1][value2:1]...
```

## Manejo de Errores

### Tipos de Error

1. **ProtocolError**
   - Mensaje malformado o longitud inválida
   - Opcode desconocido
   - Datos UTF-8 inválidos
   - Validación de campos fallida

2. **EOFError**
   - Conexión cerrada por cliente
   - Socket cerrado inesperadamente

3. **ValueError**
   - Valores fuera de rango para tipos de datos
   - Parámetros de construcción inválidos

4. **Socket Errors**
   - Timeouts de red
   - Errores de SO

### Estrategias de Recuperación

1. **Consumo de Bytes Restantes**: En caso de ProtocolError, consumir bytes restantes para limpiar el buffer del socket
2. **Validación Temprana**: Verificar opcodes y longitudes antes de procesamiento completo
3. **Logging Detallado**: Información de contexto para debugging
4. **Cleanup Automático**: Liberación de recursos en caso de error

## Validaciones del Protocolo

### Validaciones de Entrada
- Longitudes de mensaje >= 0
- Opcodes reconocidos (0-9) - **Actualizado** para incluir EOF
- Conteos de pares coinciden con claves requeridas
- Todas las claves requeridas presentes
- Datos UTF-8 válidos

### **Nuevas** Validaciones de EOF
- `table_type`: String válido que especifica tipo de tabla
- `amount`: Siempre 1 para mensajes EOF
- `batch_status`: Siempre `BatchStatus.EOF` (1)
- `rows`: Lista vacía (EOF no transporta datos reales)

### Validaciones de DataBatch
- `table_ids` y `query_ids`: máximo 255 elementos, valores 0-255
- `meta`: máximo 255 entradas, claves y valores u8
- `total_shards` y `shard_num`: valores u16 (0-65535)
- `batch_bytes`: enmarcado válido del mensaje interno

### Validaciones de Serialización
- Valores en rango para tipos de datos
- Longitudes de lista dentro de límites
- Diccionarios con claves y valores válidos

## Logging y Monitoreo

### Eventos de Protocolo
```python
"action: data_batch_to_bytes | batch_number: %d | table_ids: %s | query_ids: %s | total_shards: %d | shard_num: %d | body_size: %d bytes | final_size: %d bytes"
```

### Eventos de Error
- Errores de protocolo con contexto de opcode
- Errores de validación con detalles específicos
- Timeouts y errores de red

## Dependencias

### Módulos Estándar
- `socket` - Comunicaciones TCP
- `logging` - Sistema de logging
- `typing` - Type hints

### Sin Dependencias Externas
El protocolo está implementado usando solo la biblioteca estándar de Python para máxima portabilidad.

## Extensibilidad

### Agregar Nuevo Tipo de Mensaje

1. **Definir OpCode** en `Opcodes`
2. **Crear Entidad** en `entities.py` si es necesario
3. **Crear Clase de Mensaje** en `messages.py`
4. **Agregar a Dispatcher** en `recv_msg()`
5. **Agregar a DataBatch factory** si es embebible

### Ejemplo Implementado: EOFMessage
El mensaje EOF ya está completamente implementado siguiendo este patrón:

```python
# En constants.py
class Opcodes:
    EOF = 9

# En messages.py
class EOFMessage(TableMessage):
    def __init__(self):
        super().__init__(
            opcode=Opcodes.EOF,
            required_keys=("table_type",),
            row_factory=lambda **kwargs: None  # No crear objetos row
        )
        self.table_type = ""
    
    def create_eof_message(self, batch_number: int, table_type: str):
        """Crea mensaje EOF para un tipo específico de tabla."""
        self.amount = 1
        self.batch_number = batch_number
        self.batch_status = BatchStatus.EOF
        self.table_type = table_type
        self.rows = []
        return self
    
    def get_table_type(self) -> str:
        """Obtiene el tipo de tabla del mensaje EOF."""
        return self.table_type
    
    def to_bytes(self) -> bytes:
        """Serializa el mensaje EOF a bytes."""
        import struct
        # Implementación de serialización con struct.pack()
        # usando little-endian para compatibilidad

# En dispatcher.py
def recv_msg(sock):
    # ... 
    elif opcode == Opcodes.EOF:
        msg = EOFMessage()
    # ...
```

## Consideraciones de Rendimiento

### Optimizaciones Implementadas
- `recv_exactly()` usa `recv_into()` para evitar copias de memoria
- Parsing streaming sin cargar mensajes completos en memoria
- Validación temprana para fallar rápido
- Reutilización de buffers donde es posible

### Límites y Consideraciones
- Máximo 1MB por mensaje batch
- Máximo 255 elementos en listas u8
- Máximo 255 entradas en diccionarios u8->u8
- Límites de u16 para sharding (65535)

## Consideraciones de Seguridad

- **Input Validation**: Validación exhaustiva de todos los campos de entrada
- **Buffer Safety**: `recv_exactly()` previene buffer overflows
- **UTF-8 Safety**: Validación de encoding antes de decodificación
- **Resource Limits**: Límites en tamaños de mensaje y estructuras de datos
- **Error Handling**: Sin leak de información interna en mensajes de error

## Patrones de Diseño Utilizados

1. **Factory Pattern**: `recv_msg()` y `instantiate_message_for_opcode()`
2. **Template Method**: `TableMessage` con pasos personalizables
3. **Strategy Pattern**: Diferentes clases de mensaje por opcode
4. **Builder Pattern**: Construcción paso a paso de DataBatch
5. **Facade Pattern**: `recv_msg()` como interfaz simplificada
6. **DTO Pattern**: Entidades raw como objetos de transferencia de datos