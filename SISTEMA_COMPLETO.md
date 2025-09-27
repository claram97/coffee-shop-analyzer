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