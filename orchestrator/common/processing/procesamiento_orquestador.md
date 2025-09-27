# Documentación de Procesamiento del Orquestador - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación de la capa de procesamiento del orquestador Python en el sistema analizador de cafeterías. Esta capa proporciona un sistema completo de filtrado, serialización y transformación de datos, preparándolos para análisis downstream y transmisión a consumidores especializados.

## Estructura del Paquete

La implementación de procesamiento se encuentra en el directorio `orchestrator/common/processing/` y consta de los siguientes archivos:

- `__init__.py` - Exporta las funciones y clases principales del paquete
- `batch_processor.py` - Orquestación principal del procesamiento de lotes
- `filters.py` - Funciones de filtrado por tipo de datos
- `serialization.py` - Serialización binaria de datos filtrados  
- `file_utils.py` - Utilidades de logging y escritura de archivos para debugging

## Arquitectura del Sistema

### Exportaciones del Paquete (`__init__.py`)

```python
# Funciones de filtrado
'filter_menu_items_columns', 'filter_stores_columns', 'filter_transaction_items_columns',
'filter_transactions_columns', 'filter_users_columns',

# Funciones de serialización
'serialize_filtered_menu_items', 'serialize_filtered_stores', 'serialize_filtered_transaction_items',
'serialize_filtered_transactions', 'serialize_filtered_users',

# Procesamiento de lotes
'BatchProcessor', 'create_filtered_data_batch', 'batch_processor',

# Utilidades de archivos
'MessageLogger', 'message_logger'
```

## Componentes Principales

### 1. BatchProcessor (batch_processor.py)

Orquesta el proceso de transformación de mensajes de datos en bruto en un formato filtrado y serializado adecuado para consumidores downstream.

#### Estructura
```python
class BatchProcessor:
    def __init__(self)
    def _get_filter_functions_mapping(self) -> Dict[int, Callable]
    def _get_serialize_functions_mapping(self) -> Dict[int, Callable]
    def _get_query_mappings(self) -> Dict[int, List[int]]
    def _get_table_names_mapping(self) -> Dict[int, str]
    def get_filter_function_for_opcode(self, opcode: int) -> Callable
    def get_query_ids_for_opcode(self, opcode: int) -> List[int]
    def get_serialize_function_for_opcode(self, opcode: int) -> Callable
    def get_table_name_for_opcode(self, opcode: int) -> str
    def filter_and_serialize_data(self, original_msg)
    def create_filtered_data_batch(self, original_msg) -> DataBatch
```

#### Constructor

```python
def __init__(self):
```

**Inicialización:**
- Configura mapeos internos para funciones de filtro por opcode
- Establece funciones de serialización por opcode
- Define mapeos de query IDs por tipo de datos
- Configura nombres de tablas para logging

#### Mapeos Internos

##### Funciones de Filtrado por OpCode
```python
{
    Opcodes.NEW_MENU_ITEMS: filter_menu_items_columns,
    Opcodes.NEW_STORES: filter_stores_columns,
    Opcodes.NEW_TRANSACTION_ITEMS: filter_transaction_items_columns,
    Opcodes.NEW_TRANSACTION: filter_transactions_columns,
    Opcodes.NEW_USERS: filter_users_columns,
}
```

##### Mapeo de Query IDs
```python
{
    Opcodes.NEW_TRANSACTION: [1, 3, 4],       # Transacciones: query 1, 3, 4
    Opcodes.NEW_TRANSACTION_ITEMS: [2],        # Items de transacción: query 2  
    Opcodes.NEW_MENU_ITEMS: [2],               # Items del menú: query 2
    Opcodes.NEW_STORES: [3],                   # Tiendas: query 3
    Opcodes.NEW_USERS: [4],                    # Usuarios: query 4
}
```

##### Nombres de Tablas
```python
{
    Opcodes.NEW_MENU_ITEMS: "menu_items",
    Opcodes.NEW_STORES: "stores", 
    Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
    Opcodes.NEW_TRANSACTION: "transactions",
    Opcodes.NEW_USERS: "users",
}
```

#### Funciones Principales

##### `create_filtered_data_batch(original_msg) -> DataBatch`
Método principal que orquesta todo el proceso de filtrado y serialización.

**Proceso Completo:**
1. **Obtener Query IDs**: Determina qué queries usan estos datos
2. **Filtrar y Serializar**: Aplica filtros y serialización apropiados
3. **Crear Wrapper**: Envuelve datos en mensaje `DataBatch`
4. **Logging Completo**: Registra todo el proceso de transformación
5. **Compatibilidad**: Agrega datos para compatibilidad hacia atrás

##### `filter_and_serialize_data(original_msg)`
Aplica las funciones apropiadas de filtro y serialización a los datos de un mensaje original.

**Retorna:**
- `filtered_rows`: Lista de diccionarios de filas filtradas
- `inner_body`: Representación binaria serializada de los datos filtrados

##### `get_filter_function_for_opcode(opcode: int) -> Callable`
Obtiene la función de filtro apropiada para un opcode dado.

**Excepciones:**
- `ValueError`: Si no hay función de filtro registrada para el opcode

##### `get_serialize_function_for_opcode(opcode: int) -> Callable`
Obtiene la función de serialización apropiada para un opcode dado.

##### `get_query_ids_for_opcode(opcode: int) -> List[int]`
Obtiene la lista de query IDs asociados con un opcode dado.

**Por Defecto:** Retorna `[1]` si no se encuentra mapeo específico

#### Funciones de Logging

##### `log_data_batch_creation(original_msg, query_ids, filtered_rows, inner_body, batch_bytes)`
Registra un resumen completo del proceso de creación de lote de datos.

**Información incluida:**
- Nombre de tabla y opcode
- Query IDs relevantes  
- Conteo de filas original vs filtrado
- Número de lote
- Tamaños de datos en bytes

##### `log_filtered_sample(original_msg, filtered_rows)`
Registra una muestra de los datos filtrados para debugging (primera fila).

##### `log_data_batch_ready(original_msg)`
Registra confirmación final de que un `DataBatch` está completamente creado.

#### Funciones Auxiliares

##### `create_data_batch_wrapper(original_msg, query_ids, batch_bytes) -> DataBatch`
Construye el wrapper final del mensaje `DataBatch` alrededor de los datos serializados.

##### `add_backward_compatibility_data(wrapper, original_msg, filtered_rows)`
Adjunta datos de compatibilidad hacia atrás al wrapper `DataBatch`.

### 2. Funciones de Filtrado (filters.py)

Proporciona funciones especializadas para filtrar objetos de datos en bruto, seleccionando solo las columnas requeridas para procesamiento downstream.

#### `filter_menu_items_columns(rows: List[RawMenuItems]) -> List[Dict]`
Filtra elementos del menú seleccionando información esencial del producto.

**Columnas Retenidas:**
- `product_id` - ID del producto
- `name` - Nombre del producto  
- `price` - Precio

**Columnas Descartadas:**
- `category`, `is_seasonal`, `available_from`, `available_to`

#### `filter_stores_columns(rows: List[RawStore]) -> List[Dict]`
Filtra datos de tiendas manteniendo identificadores esenciales.

**Columnas Retenidas:**
- `store_id` - ID de la tienda
- `store_name` - Nombre de la tienda

**Columnas Descartadas:**
- `street`, `postal_code`, `city`, `state`, `latitude`, `longitude`

#### `filter_transaction_items_columns(rows: List[RawTransactionItem]) -> List[Dict]`
Filtra elementos de transacciones manteniendo detalles transaccionales clave.

**Columnas Retenidas:**
- `transaction_id` - ID de la transacción
- `item_id` - ID del elemento
- `quantity` - Cantidad
- `subtotal` - Subtotal
- `created_at` - Fecha de creación

**Columnas Descartadas:**
- `unit_price`

#### `filter_transactions_columns(rows: List[RawTransaction]) -> List[Dict]`
Filtra transacciones manteniendo identificadores centrales y monto final.

**Columnas Retenidas:**
- `transaction_id` - ID de la transacción
- `store_id` - ID de la tienda
- `user_id` - ID del usuario
- `final_amount` - Monto final
- `created_at` - Fecha de creación

**Columnas Descartadas:**
- `payment_method_id`, `original_amount`, `discount_applied`

#### `filter_users_columns(rows: List[RawUser]) -> List[Dict]`
Filtra datos de usuarios manteniendo ID y fecha de nacimiento para análisis.

**Columnas Retenidas:**
- `user_id` - ID del usuario
- `birthdate` - Fecha de nacimiento

**Columnas Descartadas:**
- `gender`, `registered_at`

### 3. Funciones de Serialización (serialization.py)

Proporciona funciones para serializar estructuras de datos filtradas en formato binario específico para transmisión de red.

#### Funciones de Serialización de Bajo Nivel

##### `serialize_header(n_rows: int, batch_number: int, batch_status: int) -> bytearray`
Serializa un encabezado común para mensajes de datos filtrados.

**Formato:** `[i32 nRows][i64 batchNumber][u8 status]`

##### `serialize_key_value_pair(key: str, value: str) -> bytearray`
Serializa un solo par clave-valor al formato `[string key][string value]`.

**Formato de String:** `[i32 length][bytes UTF-8]`

##### `serialize_row(row: Dict, required_keys: List[str]) -> bytearray`
Serializa un diccionario (fila) a formato binario.

**Formato:** `[i32 n_pairs][pair_1][pair_2]...[pair_n]`

##### `serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, table_name) -> bytes`
Función genérica que serializa una lista de filas filtradas en payload de mensaje binario completo.

#### Funciones de Serialización Específicas por Tipo

##### `serialize_filtered_menu_items(filtered_rows, batch_number, batch_status) -> bytes`
**Claves requeridas:** `["product_id", "name", "price"]`

##### `serialize_filtered_stores(filtered_rows, batch_number, batch_status) -> bytes`
**Claves requeridas:** `["store_id", "store_name"]`

##### `serialize_filtered_transaction_items(filtered_rows, batch_number, batch_status) -> bytes`
**Claves requeridas:** `["transaction_id", "item_id", "quantity", "subtotal", "created_at"]`

##### `serialize_filtered_transactions(filtered_rows, batch_number, batch_status) -> bytes`
**Claves requeridas:** `["transaction_id", "store_id", "user_id", "final_amount", "created_at"]`

##### `serialize_filtered_users(filtered_rows, batch_number, batch_status) -> bytes`
**Claves requeridas:** `["user_id", "birthdate"]`

### 4. MessageLogger (file_utils.py)

Proporciona utilidades para escribir contenidos detallados de mensajes de red a disco, útil para debugging de pipelines de procesamiento de datos.

#### Estructura
```python
class MessageLogger:
    def __init__(self, original_file: str = "received_messages.txt", filtered_file: str = "filtered_messages.txt")
    def write_original_message(self, msg: Any, status_text: str)
    def write_filtered_message(self, filtered_batch: Any, status_text: str)
    def log_batch_processing_success(self, msg: Any, status_text: str)
    def log_batch_filtered(self, filtered_batch: Any, status_text: str)
```

#### Funciones Principales

##### `write_original_message(msg, status_text)`
Anexa el contenido de un mensaje sin procesar a un archivo de log.

**Información registrada:**
- Metadatos del mensaje (opcode, amount, batch_number, status)
- Contenido completo de cada fila de datos

##### `write_filtered_message(filtered_batch, status_text)`
Anexa el contenido de un `DataBatch` procesado y filtrado a un archivo de log.

**Información registrada:**
- Metadatos de la tabla y conteos de filas
- Query IDs y Table IDs
- Información de sharding
- Contenido de filas filtradas

##### `log_batch_processing_success(msg, status_text)`
Registra resumen de alto nivel indicando que un lote fue recibido exitosamente.

##### `log_batch_filtered(filtered_batch, status_text)`
Registra resumen detallando los resultados del proceso de filtrado.

## Flujo de Operación Completo

### 1. Procesamiento Principal
```python
# Crear o usar procesador global
batch_processor = BatchProcessor()

# Procesar mensaje original
filtered_data_batch = batch_processor.create_filtered_data_batch(original_msg)

# El resultado es un DataBatch listo para transmisión
```

### 2. Proceso Interno Detallado
```python
# 1. Determinar tipo y mapeos
query_ids = get_query_ids_for_opcode(original_msg.opcode)
filter_func = get_filter_function_for_opcode(original_msg.opcode)
serialize_func = get_serialize_function_for_opcode(original_msg.opcode)

# 2. Filtrar datos
filtered_rows = filter_func(original_msg.rows)

# 3. Serializar datos filtrados  
inner_body = serialize_func(filtered_rows, original_msg.batch_number, original_msg.batch_status)

# 4. Crear wrapper DataBatch
batch_bytes = DataBatch.make_embedded(inner_opcode=original_msg.opcode, inner_body=inner_body)
wrapper = DataBatch(query_ids=query_ids, batch_bytes=batch_bytes)

# 5. Agregar compatibilidad hacia atrás
wrapper.filtered_data = {...}
```

### 3. Uso de Funciones de Conveniencia
```python
# Función global de conveniencia
from common.processing import create_filtered_data_batch

# Uso directo
filtered_batch = create_filtered_data_batch(original_msg)
```

### 4. Logging y Debugging
```python
# Usar logger global
from common.processing import message_logger

# Escribir mensajes para debugging
message_logger.write_original_message(original_msg, "Continue")
message_logger.write_filtered_message(filtered_batch, "Continue")

# Logging estándar
message_logger.log_batch_processing_success(original_msg, "EOF")
message_logger.log_batch_filtered(filtered_batch, "EOF")
```

## Mapeo de Tipos de Datos

### Tipos de Entrada (Raw Data)
| Tipo | Campos de Entrada | Campos de Salida |
|------|------------------|------------------|
| `RawMenuItems` | product_id, name, category, price, is_seasonal, available_from, available_to | product_id, name, price |
| `RawStore` | store_id, store_name, street, postal_code, city, state, latitude, longitude | store_id, store_name |
| `RawTransactionItem` | transaction_id, item_id, quantity, unit_price, subtotal, created_at | transaction_id, item_id, quantity, subtotal, created_at |
| `RawTransaction` | transaction_id, store_id, payment_method_id, user_id, original_amount, discount_applied, final_amount, created_at | transaction_id, store_id, user_id, final_amount, created_at |
| `RawUser` | user_id, gender, birthdate, registered_at | user_id, birthdate |

### Mapeo de Queries
| Tipo de Datos | Query IDs | Uso |
|---------------|-----------|-----|
| Transacciones | [1, 3, 4] | Análisis multiquery |
| Items de Transacción | [2] | Query específica de items |
| Items del Menú | [2] | Query específica de items |
| Tiendas | [3] | Query específica de tiendas |
| Usuarios | [4] | Query específica de usuarios |

## Formato de Serialización

### Estructura del Mensaje
```
[Header: i32 nRows + i64 batchNumber + u8 status]
[Row 1: i32 nPairs + [key-value pairs]]
[Row 2: i32 nPairs + [key-value pairs]]
...
[Row N: i32 nPairs + [key-value pairs]]
```

### Formato de Key-Value Pair
```
[Key: i32 length + UTF-8 bytes]
[Value: i32 length + UTF-8 bytes]
```

## Configuración y Parámetros

### Archivos de Logging
- **Mensajes Originales**: `received_messages.txt` (configurable)
- **Mensajes Filtrados**: `filtered_messages.txt` (configurable)

### Niveles de Logging
- **INFO**: Procesamiento exitoso, conteos de filas, creación de lotes
- **DEBUG**: Muestras de datos, detalles de serialización
- **WARNING**: Fallos en escritura de archivos (no críticos)

## Manejo de Errores

### Tipos de Error

1. **OpCode No Soportado (`ValueError`)**
   - No hay función de filtro/serialización registrada
   - **Acción**: Excepción propagada al caller

2. **Errores de Escritura de Archivos**
   - Permisos, espacio en disco, etc.
   - **Acción**: Warning logged, procesamiento continúa

3. **Errores de Serialización**
   - Datos malformados, encoding issues
   - **Acción**: Excepción propagada

### Estrategias de Recuperación

1. **Validación Temprana**: Verificación de opcodes antes de procesamiento
2. **Logging Defensivo**: Try/catch en operaciones de archivo
3. **Fallbacks**: Valores por defecto para campos faltantes
4. **Propagación Controlada**: Errores críticos se propagan, no críticos se logean

## Logging y Monitoreo

### Eventos de Procesamiento
```python
"action: create_data_batch | table: %s | opcode: %d | query_ids: %s | original_rows: %d | filtered_rows: %d | batch_number: %d | inner_body_size: %d bytes | batch_bytes_size: %d bytes"
```

### Eventos de Filtrado
```python
"action: batch_filtered | table: %s | original_count: %d | filtered_count: %d | batch_number: %d | status: %s"
```

### Eventos de Serialización
```python
"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes"
```

### Muestras de Datos (Debug)
```python
"action: filtered_sample | table: %s | batch_number: %d | sample_keys: %s | sample_row: %s"
```

## Dependencias

### Módulos Estándar
- `logging` - Sistema de logging de Python
- `typing` - Type hints para mejor código

### Módulos Internos
- `..protocol.Opcodes` - Códigos de operación
- `..protocol.DataBatch` - Wrapper de datos filtrados
- `..protocol.entities` - Entidades de datos raw (RawMenuItems, etc.)

## Escalabilidad y Performance

### Optimizaciones Implementadas

1. **Procesamiento Streaming**: Procesamiento fila por fila sin cargar datasets completos
2. **Mapeos Pre-computados**: Lookup tables para funciones por opcode
3. **Serialización Eficiente**: Formato binario compacto
4. **Logging Condicional**: Debug logging solo cuando necesario

### Consideraciones de Memoria

1. **Filtrado In-Place**: No duplicación innecesaria de datos
2. **Buffers Temporales**: Liberación automática después de serialización
3. **Singleton Instances**: Reutilización de procesadores y loggers

### Consideraciones de CPU

1. **Minimal Overhead**: Mapeos directos sin procesamiento complejo
2. **Efficient Serialization**: Formato binario nativo
3. **Batch Processing**: Procesamiento en lotes vs individual

## Extensibilidad

### Agregar Nuevo Tipo de Datos

1. **Definir Entidad Raw** en `..protocol.entities`
2. **Crear Función de Filtro**:
   ```python
   def filter_new_table_columns(rows: List[RawNewTable]) -> List[Dict]:
       filtered_rows = []
       for row in rows:
           filtered_row = {
               "key1": row.key1,
               "key2": row.key2
           }
           filtered_rows.append(filtered_row)
       return filtered_rows
   ```
3. **Crear Función de Serialización**:
   ```python
   def serialize_filtered_new_table(filtered_rows, batch_number, batch_status) -> bytes:
       required_keys = ["key1", "key2"]
       return serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "new_table")
   ```
4. **Agregar a BatchProcessor**:
   ```python
   # En _get_filter_functions_mapping():
   Opcodes.NEW_TABLE: filter_new_table_columns,
   
   # En _get_serialize_functions_mapping(): 
   Opcodes.NEW_TABLE: serialize_filtered_new_table,
   ```

### Personalizar Procesamiento

1. **Filtros Personalizados**: Modificar funciones de filtro existentes
2. **Serialización Personalizada**: Cambiar formatos de salida
3. **Query Mappings**: Ajustar mapeos de query IDs
4. **Logging Personalizado**: Extender MessageLogger

## Consideraciones de Seguridad

- **Input Validation**: Validación de campos requeridos en filtros
- **Safe Serialization**: Manejo seguro de strings UTF-8
- **File Operations**: Operaciones de archivo con manejo de errores
- **Memory Safety**: No retención de referencias a datos grandes
- **Logging Security**: No exposición de datos sensibles en logs

## Patrones de Diseño Utilizados

1. **Strategy Pattern**: Diferentes filtros/serializadores por tipo de datos
2. **Factory Pattern**: Mapeos de funciones por opcode  
3. **Template Method**: Flujo estándar de procesamiento con variaciones
4. **Singleton Pattern**: Instancias globales de procesador y logger
5. **Facade Pattern**: `create_filtered_data_batch` como interfaz simplificada
6. **Builder Pattern**: Construcción paso a paso de DataBatch