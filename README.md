# Desglose de Módulos

## Cliente

### `protocol/` - Capa de Protocolo
Maneja definiciones de mensajes, serialización y constantes del protocolo:

- `protocol_constants.go` - Constantes del protocolo, códigos de operación y límites
- `protocol_interfaces.go` - Interfaces principales y tipos de error
- `messages.go` - Implementación de mensajes del protocolo y parsing
- `batch_operations.go` - Funcionalidad de gestión de batches y manejo de filas
- `batch_writer.go` - Serialización y enmarcado de batches
- `serialization.go` - Funciones de serialización de datos de bajo nivel

### `common/network/` - Capa de Red
Maneja conexiones TCP y procesamiento de respuestas:

- `connection.go` - Lógica de conexión TCP con reintentos automáticos
- `response_handling.go` - Procesamiento de respuestas del servidor y mensajes de finalización

### `common/processing/` - Capa de Procesamiento
Maneja procesamiento de archivos CSV y gestión de lotes:

- `file_processing.go` - Procesamiento de archivos CSV, directorios y **envío automático de EOF**
- `batch_processing.go` - Lógica de procesamiento por lotes y envío de datos
- `handlers.go` - Manejadores específicos para cada tipo de tabla de datos

### `common/` - Componentes Principales
- `client.go` - Lógica principal del cliente y configuración

## Orquestador

### `common/protocol/` - Capa de Protocolo
Maneja definiciones de mensajes, parsing y serialización:

- `constants.py` - Constantes del protocolo, códigos de operación y excepciones
- `entities.py` - Clases de datos en bruto (RawMenuItems, RawStore, etc.)
- `parsing.py` - Utilidades de parsing binario de bajo nivel
- `messages.py` - Clases de mensaje de alto nivel (TableMessage, NewMenuItems, EOFMessage, etc.)
- `databatch.py` - Implementación compleja del mensaje DataBatch
- `dispatcher.py` - Enrutamiento de mensajes e instanciación

### `common/network/` - Capa de Red
Gestiona conexiones y manejo de mensajes:

- `connection.py` - Clases ConnectionManager y ServerManager
- `message_handling.py` - Clases MessageHandler y ResponseHandler

### `common/processing/` - Capa de Procesamiento
Maneja transformación y filtrado de datos:

- `filters.py` - Funciones de filtrado de columnas para cada tipo de tabla
- `serialization.py` - Serialización binaria de datos filtrados
- `batch_processor.py` - Clase BatchProcessor y lógica principal de procesamiento
- `file_utils.py` - MessageLogger para debugging y salida de archivos


# Clases de Mensaje del Orquestador

Define las clases de mensaje para manejo de diferentes tipos de comunicación.

## `TableMessage` (Clase Base)
Clase base para leer mensajes que contienen datos tabulares. Una tabla es una lista de registros, donde cada registro es un mapa clave-valor.

### Estructura
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

### Constructor
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

### Proceso de Parsing

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

### Mensajes Específicos de Tabla

#### `NewMenuItems(TableMessage)`
**Claves requeridas:** `("product_id", "name", "category", "price", "is_seasonal", "available_from", "available_to")`
**Row factory:** `RawMenuItems`

#### `NewStores(TableMessage)`
**Claves requeridas:** `("store_id", "store_name", "street", "postal_code", "city", "state", "latitude", "longitude")`
**Row factory:** `RawStore`

#### `NewTransactionItems(TableMessage)`
**Claves requeridas:** `("transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at")`
**Row factory:** `RawTransactionItem`

#### `NewTransactions(TableMessage)`
**Claves requeridas:** `("transaction_id", "store_id", "payment_method_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at")`
**Row factory:** `RawTransaction`

#### `NewUsers(TableMessage)`
**Claves requeridas:** `("user_id", "gender", "birthdate", "registered_at")`
**Row factory:** `RawUser`

#### `EOFMessage(TableMessage)`
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

### Mensajes de Control

#### `Finished`
Mensaje entrante FINISHED con un solo agency_id (i32 LE) en el cuerpo.

**Atributos:**
- `opcode = Opcodes.FINISHED`
- `agency_id: int` - ID de la agencia que terminó

#### `BatchRecvSuccess` y `BatchRecvFail`
Respuestas salientes de acknowledgment (cuerpo vacío).

**Métodos:**
- `write_to(sock)`: Envía `[opcode][length=0]`

## 4. DataBatch (databatch.py)

Mensaje contenedor que transporta mensajes específicos de tabla junto con metadata esencial para routing y procesamiento. Es el que se usa internamente en nuestro sistema distribuido.

##  Estructura
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

## Formato Binario

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

### Funciones Principales

### `make_embedded(inner_opcode: int, inner_body: bytes) -> bytes` (estático)
Utilidad para enmarcar un cuerpo de mensaje interno con su opcode y longitud.

**Formato:** `[u8 opcode][i32 length][body]`

### `read_from(sock: socket.socket, length: int)`
Deserializa un mensaje DataBatch desde un socket, poblando los campos de la instancia.

**Proceso:**
1. Leer cabecera específica de DataBatch
2. Leer mensaje embebido
3. Validar que no queden bytes

### `to_bytes() -> bytes`
Serializa toda la instancia DataBatch en un solo objeto bytes.

**Requiere:** `self.batch_bytes` debe estar configurado con el mensaje embebido pre-enmarcado

## Validaciones

- `table_ids` y `query_ids`: Máximo 255 elementos, valores 0-255
- `meta`: Máximo 255 entradas, claves y valores u8
- `total_shards` y `shard_num`: Valores u16 (0-65535)
- `batch_bytes`: Debe tener enmarcado válido del mensaje interno

# Dispatcher (dispatcher.py)

Proporciona la función principal de dispatcher para recibir y rutear todos los mensajes entrantes del protocolo basado en su opcode.

## `recv_msg(sock: socket.socket)`
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


#  Mensajes del Protocolo

## Mensaje Finished (Cliente→Servidor)

Indica que el cliente ha terminado de enviar todos los mensajes batch para su agencia.

**Estructura:**
- OpCode: `OpCodeFinished` (3)
- Length: 4 bytes
- Body: `[agencyId:4]`

**Uso:**
```go
msg := &Finished{AgencyId: 12345}
msg.WriteTo(writer)
```

## BatchRecvSuccess (Servidor→Cliente)

Confirmación de que un batch fue procesado exitosamente.

**Estructura:**
- OpCode: `BatchRecvSuccessOpCode` (1)
- Length: 0 bytes
- Body: (vacío)

## BatchRecvFail (Servidor→Cliente)

Confirmación negativa de que falló el procesamiento de un batch.

**Estructura:**
- OpCode: `BatchRecvFailOpCode` (2)  
- Length: 0 bytes
- Body: (vacío)

## Mensaje EOF (Cliente→Servidor)

Indica que el cliente terminó de enviar **todos los archivos de un tipo específico de tabla** (ej: "menu_items", "stores", "transactions"). Este mensaje se envía automáticamente cuando se completa el procesamiento de una carpeta completa.

**Estructura:**
- OpCode: `OpCodeEOF` (9)
- Length: Variable (depende del tamaño del table_type)
- Body: Sigue formato TableMessage con metadatos del tipo de tabla

**Formato del cuerpo:**
```
[nRows=1:4][batchNumber:8][status=1:1][n_pairs=1:4]["table_type"][table_type_value]
```

**Características:**
- **nRows**: Siempre 1 (una fila virtual con metadatos)
- **batchNumber**: Número del último batch procesado para esa tabla  
- **status**: `BatchEOF` (1) - Indica finalización
- **table_type**: String que especifica qué tabla terminó (ej: "menu_items")

# Formato de Mensajes

### Estructura General del Frame

Todos los mensajes del protocolo siguen esta estructura general:

```
[opcode:1][length:4][message_body:variable]
```

- `opcode`: 1 byte - código de operación
- `length`: 4 bytes (int32, little-endian) - longitud del cuerpo del mensaje
- `message_body`: Carga útil del mensaje de longitud variable

### Formato de Mensaje Batch

Para mensajes batch (OpCodes 4-8), el cuerpo del mensaje tiene la siguiente estructura:

```
[opcode:1][length:4][nLines:4][batchNumber:8][status:1][data:variable]
```

- `opcode`: 1 byte - código de operación
- `length`: 4 bytes (int32, little-endian) - longitud total de: nLines + batchNumber + status + data
- `nLines`: 4 bytes (int32, little-endian) - número de filas de datos en este batch
- `batchNumber`: 8 bytes (int64, little-endian) - identificador secuencial del batch
- `status`: 1 byte - estado del batch (ver tabla de Estado del Batch)
- `data`: Filas de datos serializadas de longitud variable

### Formato de Serialización de Datos

Las filas de datos se serializan como mapas de strings usando el siguiente formato:

#### Formato de String
```
[length:4][utf8_bytes:variable]
```
- `length`: 4 bytes (int32, little-endian) - número de bytes UTF-8
- `utf8_bytes`: datos de string codificados en UTF-8

#### Formato de Mapa de Strings
```
[num_pairs:4][key1:string][value1:string][key2:string][value2:string]...
```
- `num_pairs`: 4 bytes (int32, little-endian) - número de pares clave-valor
- Cada par consiste de dos strings consecutivos (clave y valor)


# Códigos de Operación (Opcodes)

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

# Estados de Lote (BatchStatus)

Define códigos de estado incluidos en mensajes de lote de datos para indicar el estado del stream.

| Estado | Valor | Descripción |
|--------|-------|-------------|
| `CONTINUE` | 0 | Más lotes de datos seguirán a este |
| `EOF` | 1 | **Lote final en el stream** - Usado en dos contextos: (1) Último batch de archivo CSV, (2) Estado en mensaje `EOFMessage` |
| `CANCEL` | 2 | Este lote fue enviado como parte de un proceso de cancelación |


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


# Flujo Detallado con EOF
```
Cliente                                 Servidor
   │                                        │
   │── menu_items/file1.csv (batches) ────►│ ✓ ACK cada batch
   │── menu_items/file2.csv (batches) ────►│ ✓ ACK cada batch  
   │── menu_items/file3.csv (batches) ────►│ ✓ ACK cada batch
   │                                        │
   │── OpCodeEOF table_type="menu_items"──►│ ✓ ACK EOF
   │                                        │
   │── stores/stores.csv (batches) ────────►│ ✓ ACK cada batch
   │── OpCodeEOF table_type="stores" ─────►│ ✓ ACK EOF  
   │                                        │
   │── OpCodeFinished [AgencyID] ──────────►│ ✓ Procesamiento completo
   │                                        │
```

## Notas de Implementación

- Todos los enteros de múltiples bytes usan orden de bytes little-endian
- Los datos de string están codificados en UTF-8
- El tamaño del batch está limitado a 1MB para prevenir problemas de memoria
- La numeración de batches es secuencial y se gestiona automáticamente
- El protocolo está diseñado para transferencia de datos de alto rendimiento con control de flujo mediante confirmaciones