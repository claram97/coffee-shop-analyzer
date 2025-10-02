# Documentación General - Sistema Analizador de Cafeterías

## Descripción del Sistema

El sistema analizador de cafeterías es una arquitectura distribuida cliente-servidor que permite la **transmisión masiva de datos de transacciones de múltiples agencias de cafeterías** hacia un orquestador central para análisis y procesamiento. El sistema utiliza un **protocolo binario personalizado** sobre TCP y está implementado con **clientes en Go y orquestador en Python**.

## Arquitectura General

### Componentes Principales

```
┌─────────────────────┐       ┌─────────────────────────────────────┐
│      CLIENTE GO     │  TCP  │        ORQUESTADOR PYTHON          │
│                     │◄─────►│                                     │
│  ┌─────────────────┐│       │ ┌─────────────┐ ┌─────────────────┐ │
│  │   Protocol      ││       │ │ Application │ │    Protocol     │ │
│  │   (Binario)     ││       │ │   Layer     │ │   (Binario)     │ │
│  └─────────────────┘│       │ └─────────────┘ └─────────────────┘ │
│  ┌─────────────────┐│       │ ┌─────────────┐ ┌─────────────────┐ │
│  │   Network       ││       │ │   Network   │ │   Processing    │ │
│  │  (Conexiones)   ││       │ │ (Servidor)  │ │  (Filtrado)     │ │
│  └─────────────────┘│       │ └─────────────┘ └─────────────────┘ │
│  ┌─────────────────┐│       │ ┌─────────────────────────────────┐ │
│  │  Processing     ││       │ │        Common Libraries         │ │
│  │ (CSV → Lotes)   ││       │ │  Network │ Processing │ Protocol │ │
│  └─────────────────┘│       │ └─────────────────────────────────┘ │
└─────────────────────┘       └─────────────────────────────────────┘

         MÚLTIPLES                        SINGLE POINT
        CLIENTES                         ORQUESTADOR
```

### Flujo de Datos General

1. **Clientes Go** → Leen archivos CSV con datos de cafeterías
2. **Procesamiento Local** → Convierten CSV a lotes binarios  
3. **Transmisión TCP** → Envían lotes via protocolo binario personalizado
4. **Orquestador Python** → Recibe, filtra y procesa datos
5. **Análisis Downstream** → Prepara datos para consultas y análisis

## Protocolo de Comunicación

### Características del Protocolo

- **Protocolo binario personalizado** sobre TCP
- **Formato little-endian** para compatibilidad multiplataforma
- **Enmarcado completo** con validación de integridad
- **Soporte para múltiples tipos de datos** de cafeterías
- **Control de flujo** mediante acknowledgments
- **Manejo de lotes grandes** con límite de 1MB por batch

### Tipos de Mensaje

| OpCode | Nombre | Dirección | Descripción |
|--------|--------|-----------|-------------|
| 1 | `BATCH_RECV_SUCCESS` | Servidor→Cliente | ACK exitoso |
| 2 | `BATCH_RECV_FAIL` | Servidor→Cliente | ACK de fallo |
| 3 | `FINISHED` | Cliente→Servidor | Fin de transmisión |
| 4 | `NEW_MENU_ITEMS` | Cliente→Servidor | Elementos del menú |
| 5 | `NEW_STORES` | Cliente→Servidor | Información de tiendas |
| 6 | `NEW_TRANSACTION_ITEMS` | Cliente→Servidor | Items de transacciones |
| 7 | `NEW_TRANSACTION` | Cliente→Servidor | Transacciones completas |
| 8 | `NEW_USERS` | Cliente→Servidor | Información de usuarios |
| 9 | `EOF` | Cliente→Servidor | Fin de tabla específica |

### Formato de Mensaje

```
[u8 opcode][i32 length][message_body]

Para lotes de datos:
[i32 nRows][i64 batchNumber][u8 status][rows...]

Cada fila:
[i32 nPairs][key1:string][value1:string]...[keyN:string][valueN:string]

String format:
[i32 length][utf8_bytes]
```

## Arquitectura del Cliente (Go)

### Componentes del Cliente

#### 1. **Protocol Layer** (`client/protocol/`)
- **Serialización/Deserialización binaria** de mensajes
- **Gestión de lotes** con límite de 1MB
- **Estados de batch**: Continue, EOF, Cancel
- **Mensajes de control**: Finished, Success/Fail responses

**Archivos principales:**
- `protocol_constants.go` - OpCodes y constantes
- `protocol_interfaces.go` - Interfaces del protocolo  
- `messages.go` - Implementación de mensajes
- `batch_operations.go` - Operaciones de lotes
- `batch_writer.go` - Escritura binaria
- `serialization.go` - Serialización de datos

#### 2. **Network Layer** (`client/common/network/`)
- **Gestión de conexiones TCP** con reintentos automáticos
- **Procesamiento asíncrono** de respuestas del servidor
- **Manejo robusto de errores** de red
- **Cierre graceful** de conexiones

**Componentes principales:**
- `ConnectionManager` - Gestión de conexiones con reintentos
- `ResponseHandler` - Procesamiento asíncrono de respuestas
- `FinishedMessageSender` - Envío de mensajes de finalización

#### 3. **Processing Layer** (`client/common/processing/`)
- **Procesamiento streaming** de archivos CSV
- **Gestión automática de lotes** respetando límites
- **Manejadores específicos** por tipo de datos
- **Mapeo automático** de directorios a tipos de datos

**Funcionalidades:**
- `BatchProcessor` - Construcción y envío de lotes
- `FileProcessor` - Procesamiento de archivos y directorios
- `TableTypeHandler` - Mapeo automático de tipos
- 5 **manejadores específicos** para cada tipo de datos

### Flujo del Cliente

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Archivos CSV   │    │   Procesamiento │    │   Transmisión   │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │menu_items/  │ │───►│ │Leer CSV     │ │───►│ │Enviar lotes │ │
│ │stores/      │ │    │ │Crear lotes  │ │    │ │Recibir ACKs │ │
│ │transactions/│ │    │ │Serializar   │ │    │ │Manejar error│ │
│ │users/       │ │    │ │Validar      │ │    │ │Enviar FINISH│ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘

     ENTRADA               PROCESAMIENTO           SALIDA
```

### Proceso Detallado del Cliente

1. **Inicialización**
   - Configuración desde `config.yaml`
   - Establecimiento de conexión TCP con reintentos
   - Configuración de manejadores por tipo de datos

2. **Procesamiento de Archivos**
   - Navegación por estructura de directorios
   - Mapeo automático: `menu_items/` → `MenuItemHandler`
   - Lectura streaming de CSV (línea por línea)
   - Construcción de lotes respetando límite de 1MB

3. **Transmisión de Datos**
   - Serialización binaria de lotes
   - Envío con numeración secuencial de batches
   - Recepción asíncrona de acknowledgments
   - Estados: Continue → Continue → ... → EOF

4. **Finalización**
   - Envío de mensaje `FINISHED` con Agency ID
   - Cierre graceful de conexión
   - Cleanup de recursos

## Arquitectura del Orquestador (Python)

### Componentes del Orquestador

#### 1. **Application Layer** (`orchestrator/app/`)
- **Orquestación modular** con separación de responsabilidades
- **Registro de procesadores** por tipo de mensaje
- **Integración preparada** para colas de mensajes
- **Logging detallado** del procesamiento

**Características:**
- `Orchestrator` - Clase principal con arquitectura modular
- **Procesamiento por OpCode** con delegación automática
- **Sistema de filtrado** integrado
- **Logging configurable** para debugging/producción

#### 2. **Network Layer** (`orchestrator/common/network/`)
- **Arquitectura thread-per-client** para concurrencia
- **Shutdown graceful** mediante señales SIGTERM
- **Dispatching flexible** por opcode
- **Manejo aislado de errores** por cliente

**Componentes:**
- `ServerManager` - Gestión de alto nivel con señales
- `ConnectionManager` - Gestión de bajo nivel de conexiones
- `MessageHandler` - Dispatching por opcode
- `ResponseHandler` - Respuestas estandarizadas

#### 3. **Processing Layer** (`orchestrator/common/processing/`)
- **Sistema de filtrado inteligente** que reduce datos
- **Serialización binaria eficiente** para downstream
- **Mapeo automático** por OpCode a funciones
- **Sistema de Query IDs** para análisis específicos

**Funcionalidades:**
- `BatchProcessor` - Orquestación de filtrado y serialización
- 5 **funciones de filtrado** específicas por tipo
- 5 **funciones de serialización** binaria
- `MessageLogger` - Utilidades de debugging

#### 4. **Protocol Layer** (`orchestrator/common/protocol/`)
- **Protocolo binario completo** con validación exhaustiva
- **Sistema de mensajes extensible** con factory patterns
- **Soporte para datos tabulares** con esquemas automáticos
- **Parsing streaming eficiente** sin carga completa en memoria

**Componentes:**
- `DataBatch` - Mensaje contenedor con metadata de routing
- `TableMessage` - Base para mensajes tabulares
- 5 **clases de mensaje** específicas por tipo de datos
- **Funciones de parsing** de bajo nivel

### Flujo del Orquestador

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Recepción     │    │  Procesamiento  │    │     Salida      │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │Aceptar conn │ │───►│ │Parse mensaje│ │───►│ │Datos filtrd │ │
│ │Leer mensaje │ │    │ │Filtrar datos│ │    │ │Serialización│ │
│ │Dispatch     │ │    │ │Serializar   │ │    │ │Query routing│ │
│ │Validar      │ │    │ │Logging      │ │    │ │Enviar ACK   │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘

    TCP SERVER           DATA PROCESSING         DOWNSTREAM
```

## Flujo Completo Cliente → Orquestador

### 1. Establecimiento de Conexión

```
Cliente Go                               Orquestador Python
    │                                            │
    │──────────── TCP Connect ──────────────────►│
    │                                            │ ServerManager.accept()
    │                                            │ ConnectionManager.create_thread()
    │◄─────────── Connection OK ────────────────│
    │                                            │
```

### 2. Transmisión de Datos

```
Cliente Go                               Orquestador Python
    │                                            │
    │ FileProcessor.processTableType()           │
    │ ┌──────────────────────────────┐          │
    │ │ Para cada archivo CSV:       │          │
    │ │   ┌────────────────────────┐ │          │
    │ │   │ Leer línea por línea   │ │          │
    │ │   │ Crear lotes ≤ 1MB      │ │          │
    │ │   │ Serializar binario     │ │          │
    │ │   └────────────────────────┘ │          │
    │ └──────────────────────────────┘          │
    │                                            │
    │──── NEW_MENU_ITEMS [Batch 1] ────────────►│
    │                                            │ recv_msg()
    │                                            │ MessageHandler.handle_message()
    │                                            │ BatchProcessor.create_filtered_batch()
    │◄───── BATCH_RECV_SUCCESS ─────────────────│ ResponseHandler.send_success()
    │                                            │
    │──── NEW_MENU_ITEMS [Batch 2] ────────────►│
    │◄───── BATCH_RECV_SUCCESS ─────────────────│
    │                                            │
    │──── NEW_MENU_ITEMS [Batch N, EOF] ───────►│
    │◄───── BATCH_RECV_SUCCESS ─────────────────│
    │                                            │
    │ (Repetir para stores, transactions, etc.)  │
    │                                            │
```

### 3. Finalización

```
Cliente Go                               Orquestador Python
    │                                            │
    │──────── FINISHED [AgencyID] ─────────────►│
    │                                            │ Procesa FINISHED
    │                                            │ Retorna False (cerrar)
    │◄──────── Connection Close ────────────────│
    │                                            │
```

## Transformación de Datos

### Filtrado Inteligente por Tipo

El orquestador aplica **filtrado inteligente** que retiene solo campos esenciales:

| Tipo Original | Campos Originales | Campos Filtrados | Reducción |
|---------------|------------------|------------------|-----------|
| **MenuItems** | 7 campos | 3 campos (id, name, price) | 57% |
| **Stores** | 8 campos | 2 campos (id, name) | 75% |  
| **TransactionItems** | 6 campos | 5 campos (id, item_id, qty, subtotal, date) | 17% |
| **Transactions** | 8 campos | 5 campos (id, store_id, user_id, amount, date) | 37% |
| **Users** | 4 campos | 2 campos (id, birthdate) | 50% |

### Mapeo de Queries

Los datos filtrados se asocian automáticamente con **Query IDs** específicos:

| Tipo de Datos | Query IDs | Propósito de Análisis |
|---------------|-----------|----------------------|
| **Transactions** | [1, 3, 4] | Análisis multiquery: general + tiendas + usuarios |
| **TransactionItems** | [2] | Análisis específico de productos vendidos |
| **MenuItems** | [2] | Análisis específico de productos disponibles |
| **Stores** | [3] | Análisis específico por ubicación geográfica |
| **Users** | [4] | Análisis demográfico por edad |

## Manejo de Errores y Recuperación

### Errores del Cliente

1. **Errores de Conexión**
   - **Estrategia**: Reintentos automáticos con backoff
   - **Configuración**: Número de intentos y intervalo configurable
   - **Recovery**: Fallo solo después de agotar reintentos

2. **Errores de Procesamiento CSV**
   - **Estrategia**: Continuar con otros archivos
   - **Logging**: Error detallado pero no crítico
   - **Recovery**: Procesar archivos restantes

3. **Errores de Red durante Transmisión**
   - **Detección**: `IsConnectionError()` identifica tipos de error
   - **Acción**: Terminación controlada, cleanup de recursos
   - **Logging**: Error crítico con contexto completo

### Errores del Orquestador

1. **Errores de Protocolo**
   - **Aislamiento**: Error en un cliente no afecta otros
   - **Cleanup**: Consumo de bytes restantes para limpiar buffer
   - **Response**: Envío de `BATCH_RECV_FAIL`

2. **Errores de Procesamiento**
   - **Estrategia**: Continuar procesamiento, enviar FAIL
   - **Logging**: Error detallado con contexto de lote
   - **Recovery**: Mantener conexión, procesar siguientes mensajes

3. **Errores de Sistema**
   - **SIGTERM**: Shutdown graceful con espera de threads
   - **Resource exhaustion**: Límites preventivos en memoria
   - **Network issues**: Timeouts configurables

## Características de Rendimiento

### Optimizaciones del Cliente

1. **Streaming Processing**
   - Lectura línea por línea de CSV (no carga completa)
   - Construcción incremental de lotes
   - Liberación automática de memoria

2. **Network Efficiency**
   - Protocolo binario compacto vs. JSON/XML
   - Lotes de hasta 1MB para eficiencia de red
   - Conexiones TCP persistentes

3. **Concurrency**
   - Gorrutina dedicada para lectura de respuestas
   - Procesamiento asíncrono de acknowledgments
   - Context support para cancelación coordinada

### Optimizaciones del Orquestador

1. **Concurrent Processing**
   - Thread por cliente para aislamiento
   - Procesamiento paralelo de múltiples clientes
   - Event-driven shutdown

2. **Memory Management**
   - Streaming parsing sin carga completa
   - Liberación automática de buffers después de procesamiento
   - Límites preventivos (1MB por mensaje)

3. **Data Processing**
   - Filtrado temprano reduce carga downstream
   - Serialización binaria eficiente
   - Mapeos pre-computados para lookup rápido

## Configuración del Sistema

### Cliente (config.yaml)
```yaml
id: "agencia-1"
server:
  address: "orquestador:8080"
batch:
  maxAmount: 1000
log:
  level: "INFO"
```

### Orquestador (config.ini)
```ini
[SERVER]
PORT=8080
LISTEN_BACKLOG=10

[LOGGING]
LOGGING_LEVEL=INFO
```

## Logging y Monitoreo

### Eventos del Cliente
```
action: connect | result: success | client_id: agencia-1 | attempt: 1
action: processing_file | file: menu_items/items.csv
action: batch_enviado | result: success
action: send_finished | result: success | agencyId: 1
```

### Eventos del Orquestador
```
action: accept_connections | result: success | ip: 172.25.1.2
action: receive_message | result: success | ip: 172.25.1.2 | opcode: 4
action: create_data_batch | table: menu_items | original_rows: 1000 | filtered_rows: 1000
action: batch_filtered | table: menu_items | original_count: 1000 | filtered_count: 1000
```

## Escalabilidad y Futuras Mejoras

### Escalabilidad Horizontal

1. **Múltiples Clientes**
   - Soporte nativo para N clientes simultáneos
   - Aislamiento completo por thread
   - Sin límite teórico de clientes (limitado por OS)

2. **Load Balancing**
   - Múltiples instancias de orquestador
   - Distribución de clientes por DNS/proxy
   - Estado compartido mínimo

### Mejoras Preparadas

1. **Message Queues**
   ```python
   # Ya preparado en código
   # self._filter_router_queue.send(batch_bytes)
   ```

2. **Sharding Support**
   ```python
   # DataBatch ya incluye metadata de sharding
   total_shards: int
   shard_num: int
   ```

3. **Monitoring Integration**
   - Métricas de throughput por cliente
   - Alertas de errores de conexión
   - Dashboard de procesamiento en tiempo real

## Casos de Uso del Sistema

### Caso de Uso 1: Sincronización Diaria
- **Escenario**: 50 agencias envían datos diarios
- **Volumen**: ~1M transacciones/día por agencia
- **Procesamiento**: Filtrado automático, routing por query
- **Resultado**: Datos preparados para análisis de tendencias

### Caso de Uso 2: Análisis en Tiempo Real
- **Escenario**: Stream continuo de transacciones
- **Latencia**: <100ms por lote
- **Processing**: Filtrado por tipo, enrutado por Query ID
- **Resultado**: Datos listos para dashboards en vivo

### Caso de Uso 3: Migración de Datos
- **Escenario**: Migración masiva desde sistemas legacy
- **Volumen**: TB de datos históricos
- **Reliability**: Reintentos automáticos, recovery de errores
- **Resultado**: Datos migrados con integridad garantizada

## Ventajas del Sistema

1. **Performance Superior**
   - Protocolo binario vs. REST/JSON
   - Streaming processing vs. batch loading
   - Concurrencia nativa en ambos extremos

2. **Reliability**
   - Reintentos automáticos en cliente
   - Acknowledgments por lote
   - Manejo robusto de errores

3. **Scalability**
   - Thread-per-client architecture
   - Horizontal scaling ready
   - Resource limits preventivos

4. **Maintainability**
   - Arquitectura modular bien definida
   - Separación clara de responsabilidades
   - Extensive logging y monitoreo

5. **Extensibility**
   - Fácil adición de nuevos tipos de datos
   - Protocol extensible con OpCodes
   - Plugin architecture preparada

## Conclusiones

El sistema analizador de cafeterías representa una **arquitectura distribuida robusta y eficiente** para la transmisión masiva de datos transaccionales. La combinación de **clientes Go de alto rendimiento** con un **orquestador Python modular** proporciona una solución escalable que puede manejar **múltiples agencias transmitiendo datos simultáneamente**.

### Fortalezas Clave:

1. **Protocolo binario personalizado** optimizado para el dominio específico
2. **Arquitectura modular** que facilita mantenimiento y extensión
3. **Manejo robusto de errores** con recovery automático
4. **Procesamiento streaming** que minimiza uso de memoria
5. **Preparación para escalabilidad** con message queues y sharding

El sistema está **listo para producción** y puede evolucionar para satisfacer requisitos futuros de análisis de datos más complejos en el dominio de cafeterías y retail.

## Sistema de Señalización EOF

### Descripción General

El sistema implementa un **mecanismo de señalización EOF (End of File) automático** que permite al orquestador saber cuándo un cliente ha terminado de enviar todos los archivos de un tipo específico de datos (table type). Este mecanismo es crucial para el procesamiento downstream y la sincronización de datos.

### Formatos Generales de Mensajes del Protocolo

Antes de abordar el EOF específicamente, es importante entender los **dos tipos principales de mensajes** que maneja el sistema:

#### 1. TableMessage - Formato Base para Datos Tabulares

**Propósito**: Transportar datos estructurados en filas y columnas (menu_items, stores, transactions, etc.)

**Formato de Red (Cliente → Orquestador)**:
```
[OpCode:u8][MessageLength:i32][TableMessageBody]

donde TableMessageBody =
[nRows:i32][batchNumber:i64][status:u8][rows...]

donde cada row =
[n_pairs:i32]([key:string][value:string])+
```

**Características**:
- **OpCodes**: 1-8 para diferentes tipos de datos (NEW_MENU_ITEMS=1, NEW_STORES=2, etc.)
- **nRows**: Cantidad de registros en este mensaje
- **batchNumber**: Número secuencial del batch del cliente
- **status**: Estado del batch (0=Continue, 1=EOF, 2=Cancel)
- **Strings**: Codificados como bytes UTF-8 terminados en null (`\x00`)

**Ejemplo**: Mensaje NEW_MENU_ITEMS con 2 productos:
```
[0x01][0x0000012C][0x00000002][0x0000000000000001][0x00][0x00000007]
["product_id\x00"]["PROD001\x00"]["name\x00"]["Cappuccino\x00"]...
```

#### 2. DataBatch - Formato Contenedor con Metadata

**Propósito**: Encapsular cualquier mensaje con metadata de routing, sharding y distribución

**Formato Completo**:
```
[OpCode=0:u8][MessageLength:i32][DataBatchBody]

donde DataBatchBody =
[table_ids:u8_list][query_ids:u8_list][reserved:u16][batch_number:i64]
[meta:dict<u8,u8>][total_shards:u16][shard_num:u16][embedded_message]

donde embedded_message =  
[inner_opcode:u8][inner_length:i32][inner_body]
```

**Características**:
- **OpCode**: Siempre 0 (DATA_BATCH)
- **table_ids**: Lista de IDs de tablas afectadas por este mensaje
- **query_ids**: Lista de IDs de queries que deben procesar este dato
- **meta**: Diccionario de metadatos arbitrarios para extensibilidad
- **sharding**: total_shards y shard_num para distribución horizontal
- **embedded_message**: Cualquier otro mensaje del protocolo (TableMessage, Finished, etc.)

**Flujo de Uso**:
1. **Cliente envía**: TableMessage directamente al orquestador
2. **Orquestador procesa**: Filtra, enriquece y encapsula en DataBatch
3. **Orquestador reenvía**: DataBatch a colas downstream para procesamiento distribuido

### Protocolo de Mensajes EOF

#### Formato del Mensaje EOF

El mensaje EOF involucra ambos formatos: como TableMessage del cliente y como DataBatch hacia las colas.

##### 1. EOF como TableMessage (Cliente → Orquestador)

El cliente Go construye y envía un mensaje EOF siguiendo el formato TableMessage estándar:

```
[OpCode=9:u8][MessageLength:i32][TableMessageBody]

donde TableMessageBody =
[nRows=1:i32][batchNumber:i64][status=1:u8][n_pairs=1:i32]["table_type":string][table_type_value:string]
```

**Ejemplo de construcción en Go:**
```go
func (fp *FileProcessor) buildEOFMessageBody(tableType string) []byte {
    var buffer []byte
    
    // nRows = 1 (una fila con metadatos)
    nRowsBytes := make([]byte, 4)
    binary.LittleEndian.PutUint32(nRowsBytes, 1)
    buffer = append(buffer, nRowsBytes...)
    
    // batchNumber = último batch procesado
    batchBytes := make([]byte, 8) 
    binary.LittleEndian.PutUint64(batchBytes, uint64(fp.lastBatchNumber))
    buffer = append(buffer, batchBytes...)
    
    // status = BatchEOF (1)
    buffer = append(buffer, byte(1))
    
    // n_pairs = 1 (solo el par table_type)
    pairsBytes := make([]byte, 4)
    binary.LittleEndian.PutUint32(pairsBytes, 1)
    buffer = append(buffer, pairsBytes...)
    
    // "table_type" key
    keyBytes := []byte("table_type")
    buffer = append(buffer, keyBytes...)
    buffer = append(buffer, 0) // null terminator
    
    // table type value
    valueBytes := []byte(tableType)
    buffer = append(buffer, valueBytes...)
    buffer = append(buffer, 0) // null terminator
    
    return buffer
}
```

**Características del mensaje de red:**
- **OpCode**: 9 (`EOF`) - Identificador del tipo de mensaje
- **MessageLength**: Tamaño total del `TableMessageBody` en bytes
- **nRows**: 1 (una fila virtual con información de metadatos)
- **batchNumber**: Número del último batch enviado para esa tabla
- **status**: `BatchStatus.EOF` (1) - Indica fin de datos
- **n_pairs**: 1 (un solo par clave-valor)
- **Contenido**: Par `{"table_type": "nombre_tabla"}` con strings null-terminated

##### 2. EOF como DataBatch (Orquestador → Cola)

El orquestador puede procesar el EOF de dos maneras según el caso de uso:

**Opción A: Reenvío Directo (Preservando TableMessage)**
```python
def _process_eof_message(self, msg, client_sock) -> bool:
    # Reenvío directo del TableMessage serializado
    message_bytes = msg.to_bytes()  # Solo TableMessageBody
    self._filter_router_queue.send(message_bytes)
```

**Opción B: Encapsulación en DataBatch (Para Procesamiento Distribuido)**

```python
# Crear DataBatch wrapper para EOF
def create_eof_databatch(eof_msg, table_type):
    # 1. Serializar EOF como TableMessage
    eof_body = eof_msg.to_bytes()  # TableMessageBody sin OpCode/Length
    
    # 2. Crear mensaje embebido con framing
    embedded_eof = DataBatch.make_embedded(Opcodes.EOF, eof_body)
    
    # 3. Crear DataBatch con metadata apropiada
    databatch = DataBatch(
        table_ids=[get_table_id(table_type)],  # ID numérico de la tabla
        query_ids=[],  # EOF no requiere queries específicas
        meta={1: 1},  # Metadata: {EOF_SIGNAL: TRUE}
        total_shards=1,  # EOF no se fragmenta
        shard_num=0,
        batch_bytes=embedded_eof
    )
    
    return databatch.to_bytes()  # Formato completo DataBatch

def to_bytes(self) -> bytes:
    """Serializa EOF como TableMessage (para reenvío directo)."""
    
    # Preparar datos
    table_type_bytes = self.table_type.encode('utf-8')
    key_bytes = b"table_type"
    
    # Construir TableMessageBody (sin OpCode/Length de red)
    message = struct.pack('<I', 1)  # nRows = 1
    message += struct.pack('<Q', 0)  # batchNumber = 0 (EOF especial)  
    message += struct.pack('<B', 1)  # status = BatchEOF
    message += struct.pack('<I', 1)  # n_pairs = 1
    
    # Agregar par clave-valor
    message += key_bytes + b'\x00'  # "table_type" + null
    message += table_type_bytes + b'\x00'  # valor + null
    
    return message
```

**Características del mensaje de cola:**
- **Formato**: Bytes serializados con `struct.pack()` usando little-endian (`<`)
- **nRows**: 1 (fijo para EOF)
- **batchNumber**: 0 (valor especial para EOF, no representa batch real)
- **status**: 1 (`BatchStatus.EOF`)
- **Contenido**: Mismo par `{"table_type": "valor"}` preservado
- **Codificación**: UTF-8 para strings, null-terminated para compatibilidad

##### Comparación de Formatos EOF

| Aspecto | TableMessage (Cliente→Orquestador) | DataBatch (Orquestador→Cola) |
|---------|-----------------------------------|----------------------------|
| **Encabezado Red** | `[OpCode=9][Length][Body]` | `[OpCode=0][Length][Body]` |
| **Contenido** | `TableMessageBody` directo | `DataBatchBody` + embedded EOF |
| **Metadata** | Solo datos de tabla EOF | + routing, sharding, queries |
| **BatchNumber** | Último batch real procesado | Preservado del original |
| **Propósito** | Comunicación cliente-servidor | Distribución y procesamiento |
| **Sharding** | No aplicable | Soporte completo para fragments |
| **Routing** | Basado en OpCode | Basado en table_ids/query_ids |

##### Casos de Uso por Formato

**TableMessage EOF (Reenvío Directo)**:
- ✅ **Logging y auditoría** simple
- ✅ **Notificaciones** síncronas 
- ✅ **Checkpointing** básico de completitud
- ⚠️ **Limitado** para arquitecturas distribuidas

**DataBatch EOF (Procesamiento Distribuido)**:
- ✅ **Microservicios** con routing inteligente
- ✅ **Análisis cross-table** coordinado
- ✅ **Triggers** de procesamiento por query específica
- ✅ **Escalabilidad** horizontal con sharding

#### Tipos de Tabla Soportados

| Table Type | Descripción | Archivos Típicos |
|------------|-------------|------------------|
| `"menu_items"` | Elementos del menú | `menu_items/*.csv` |
| `"stores"` | Información de tiendas | `stores/*.csv` |
| `"transactions"` | Transacciones completas | `transactions/*.csv` |
| `"transaction_items"` | Items de transacciones | `transaction_items/*.csv` |
| `"users"` | Información de usuarios | `users/*.csv` |

### Implementación en el Cliente (Go)

#### Flujo Automático de EOF

```go
// En FileProcessor.ProcessTableType()
func (fp *FileProcessor) ProcessTableType(ctx context.Context, dataDir, tableType string, ...) error {
    // 1. Procesar todos los archivos CSV de la tabla
    for _, fileEntry := range files {
        if !fileEntry.IsDir() && filepath.Ext(fileEntry.Name()) == ".csv" {
            if err := fp.ProcessFile(ctx, subDirPath, fileEntry.Name(), processor); err != nil {
                return err
            }
        }
    }
    
    // 2. Enviar EOF automáticamente después de procesar todos los archivos
    if err := fp.sendEOFMessage(processor, tableType); err != nil {
        return err
    }
    
    return nil
}
```

#### Construcción del Mensaje EOF

```go
func (fp *FileProcessor) buildEOFMessageBody(tableType string) []byte {
    // TableMessage format: [nRows:i32][batchNumber:i64][status:u8][rows...]
    
    // nRows = 1 (una fila virtual con información de tabla)
    // batchNumber = 0 (EOF no tiene batch específico) 
    // status = BatchEOF (1)
    // Row: n_pairs=1, ["table_type", tableType]
    
    // Usar encoding/binary con LittleEndian para compatibilidad
}
```

### Implementación en el Orquestador (Python)

#### Clase EOFMessage

```python
class EOFMessage(TableMessage):
    """EOF message que hereda de TableMessage y especifica el tipo de tabla."""
    
    def __init__(self):
        super().__init__(
            opcode=Opcodes.EOF,
            required_keys=("table_type",),
            row_factory=lambda **kwargs: None
        )
        self.table_type = ""
    
    def get_table_type(self) -> str:
        """Obtiene el tipo de tabla del mensaje EOF."""
        return self.table_type
    
    def to_bytes(self) -> bytes:
        """Serializa el mensaje EOF a bytes usando struct.pack()."""
        # Implementación usando struct.pack() para generar bytes directamente
        # sin depender de funciones de socket
```

#### Procesamiento del EOF

```python
def _process_eof_message(self, msg, client_sock) -> bool:
    """Procesa mensajes EOF enviándolos a la cola de filtro y routing."""
    
    table_type = msg.get_table_type()
    
    # Log recepción del EOF
    logging.info(f"EOF received for table: {table_type}")
    
    # Serializar mensaje a bytes (preservando formato original)
    message_bytes = msg.to_bytes()
    
    # Enviar a cola de downstream processing
    self._filter_router_queue.send(message_bytes)
    
    # Responder éxito al cliente
    ResponseHandler.send_success(client_sock)
    return True
```

### Flujo Completo del Sistema EOF

```
Cliente Go                                Orquestador Python
    │                                            │
    │ ┌─ Procesar menu_items/ ────────┐         │
    │ │   file1.csv → Batches 1-5     │         │
    │ │   file2.csv → Batches 6-8     │         │
    │ │   file3.csv → Batches 9-12    │         │
    │ └───────────────────────────────┘         │
    │                                            │
    │──── EOF {"table_type": "menu_items"} ────►│
    │                                            │ _process_eof_message()
    │                                            │ → filter_router_queue
    │◄───── BATCH_RECV_SUCCESS ─────────────────│
    │                                            │
    │ ┌─ Procesar stores/ ─────────────┐         │
    │ │   stores.csv → Batches 13-15   │         │
    │ └───────────────────────────────┘         │
    │                                            │
    │──── EOF {"table_type": "stores"} ────────►│
    │◄───── BATCH_RECV_SUCCESS ─────────────────│
    │                                            │
    │ (Continúa con transactions, users, etc.)   │
    │                                            │
    │──────── FINISHED [AgencyID] ─────────────►│
```

### Beneficios del Sistema EOF

#### 1. **Sincronización Precisa**
- **Demarcación clara** del final de cada tipo de datos
- **Correlación automática** entre batches y tablas
- **Preparación para procesamiento** downstream por tabla

#### 2. **Monitoreo y Debugging**
```python
# Logs automáticos del sistema
"action: eof_received | result: success | table_type: menu_items | batch_number: 12"
"action: eof_forwarded | result: success | table_type: menu_items | bytes_length: 45"
```

#### 3. **Integración con Message Queues**
- **Envío automático** a colas de procesamiento
- **Preservación del formato** original del mensaje
- **Routing inteligente** basado en table_type

#### 4. **Escalabilidad y Extensión**
- **Fácil adición** de nuevos tipos de tabla
- **Compatible** con arquitecturas de microservicios
- **Preparado para sharding** por tipo de datos

### Casos de Uso Downstream

#### 1. **Triggers de Procesamiento**
```python
# El sistema downstream puede activar procesamiento específico:
if message.table_type == "transactions":
    trigger_revenue_analysis()
elif message.table_type == "menu_items":
    trigger_menu_optimization()
```

#### 2. **Checkpointing y Recovery**
```python
# Marcar completitud por cliente y tabla:
checkpoint_manager.mark_complete(
    client_id=msg.agency_id,
    table_type=msg.table_type,
    timestamp=datetime.now()
)
```

#### 3. **Análisis Cross-Table**
```python
# Esperar que todas las tablas estén completas antes de análisis:
if all_tables_complete(client_id):
    start_comprehensive_analysis(client_id)
```

El sistema EOF proporciona **granularidad perfecta** para el control de flujo y sincronización en arquitecturas distribuidas de análisis de datos masivos.