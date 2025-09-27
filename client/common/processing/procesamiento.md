# Documentación de Procesamiento - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación de la capa de procesamiento del cliente Go en el sistema analizador de cafeterías. Esta capa maneja el procesamiento de archivos CSV, la gestión de lotes de datos y la implementación de manejadores específicos para diferentes tipos de tablas.

## Estructura del Paquete

La implementación de procesamiento se encuentra en el directorio `client/common/processing/` y consta de los siguientes archivos:

- `batch_processing.go` - Maneja la lógica de procesamiento por lotes y envío de datos
- `file_processing.go` - Gestiona el procesamiento de archivos CSV y directorios
- `handlers.go` - Define manejadores específicos para cada tipo de tabla de datos

## Componentes Principales

### 1. BatchProcessor (batch_processing.go)

Maneja la lógica de procesamiento por lotes, incluyendo la lectura de CSV, construcción de lotes y envío al servidor.

#### Estructura
```go
type BatchProcessor struct {
    conn       net.Conn          // Conexión TCP al servidor
    handler    TableRowHandler   // Manejador específico del tipo de tabla
    opCode     byte              // Código de operación del protocolo
    batchLimit int32             // Límite de filas por lote
    clientID   string            // Identificador del cliente
    log        *logging.Logger   // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewBatchProcessor(conn net.Conn, handler TableRowHandler, opCode byte, batchLimit int32, clientID string, logger *logging.Logger) *BatchProcessor`
Crea una nueva instancia del procesador de lotes.

**Parámetros:**
- `conn`: Conexión TCP activa al servidor
- `handler`: Manejador específico para el tipo de datos
- `opCode`: Código de operación del protocolo
- `batchLimit`: Número máximo de filas por lote
- `clientID`: Identificador único del cliente
- `logger`: Instancia del logger

##### `BuildAndSendBatches(ctx context.Context, reader *csv.Reader, batchNumber int64) error`
Procesa el CSV de forma streaming, construyendo lotes incrementalmente y enviándolos al servidor.

**Comportamiento:**
- Lee el CSV línea por línea
- Construye lotes respetando límites de tamaño
- Envía lotes automáticamente cuando se alcanza el límite
- Maneja cancelación de contexto
- Envía lote final al llegar a EOF

**Manejo de Estados:**
- **Cancelación**: Envía lote parcial con estado `BatchCancel`
- **EOF**: Envía lote final con estado `BatchEOF`
- **Continuar**: Envía lotes intermedios con estado `BatchContinue`

#### Funciones Auxiliares

##### `processNextRow(reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, batchNumber *int64) error`
Procesa una sola fila CSV, la convierte a mapa de protocolo y la agrega al lote actual.

##### `handleCancellation(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error`
Maneja la cancelación de contexto y envía lote pendiente si hay datos.

##### `handleEOF(batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error`
Maneja el fin de archivo y envía lote pendiente si hay datos.

##### `processCSVLoop(ctx context.Context, reader *csv.Reader, batchBuff *bytes.Buffer, counter *int32, currentBatchNumber *int64) error`
Ejecuta el bucle principal de procesamiento CSV con verificación de cancelación.

### 2. FileProcessor (file_processing.go)

Gestiona la lógica de procesamiento de archivos CSV y navegación de directorios.

#### Estructura
```go
type FileProcessor struct {
    clientID string            // Identificador del cliente
    log      *logging.Logger   // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewFileProcessor(clientID string, logger *logging.Logger) *FileProcessor`
Crea una nueva instancia del procesador de archivos.

##### `ProcessFile(ctx context.Context, dir, fileName string, processor *BatchProcessor) error`
Procesa un único archivo CSV.

**Comportamiento:**
- Abre y prepara el archivo CSV
- Configura el lector CSV y omite encabezados
- Procesa el archivo de forma asíncrona
- Maneja errores y logging detallado

##### `ProcessTableType(ctx context.Context, dataDir, tableType string, processorFactory func(TableRowHandler, byte) *BatchProcessor) error`
Procesa todos los archivos CSV de un tipo de tabla específico.

**Comportamiento:**
- Determina el manejador y opcode apropiado para el tipo de tabla
- Crea procesador específico usando factory function
- Procesa todos los archivos .csv del directorio
- Incluye delay de 1 segundo entre archivos

##### `ProcessAllTables(ctx context.Context, processorFactory func(TableRowHandler, byte) *BatchProcessor, dataDir string) error`
Procesa todos los tipos de tabla en el directorio de datos.

**Comportamiento:**
- Itera sobre todos los subdirectorios
- Procesa cada tipo de tabla
- Maneja errores de conexión de forma crítica
- Continúa procesando otros tipos si hay errores no críticos

#### Funciones Auxiliares

##### `openAndPrepareFile(filePath string) (*os.File, error)`
Abre el archivo y registra el inicio del procesamiento.

##### `setupCSVReader(file *os.File) (*csv.Reader, error)`
Configura el lector CSV y omite el encabezado.

**Configuración del CSV:**
- Delimitador: coma (`,`)
- Campos por registro: variable (`-1`)
- Omite primera fila (encabezado)

##### `processFileAsync(ctx context.Context, reader *csv.Reader, fileName string, processor *BatchProcessor) error`
Procesa el archivo de forma asíncrona y maneja los resultados.

### 3. TableTypeHandler (file_processing.go)

Gestiona la configuración de tipos de tabla y mapeo de manejadores.

#### Estructura
```go
type TableTypeHandler struct {
    clientID string            // Identificador del cliente
    log      *logging.Logger   // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewTableTypeHandler(clientID string, logger *logging.Logger) *TableTypeHandler`
Crea una nueva instancia del manejador de tipos de tabla.

##### `GetHandlerAndOpCode(tableType string) (TableRowHandler, byte, error)`
Determina automáticamente el manejador y opcode apropiado basado en el nombre del directorio.

**Mapeo de Tipos:**
| Tipo de Tabla | Manejador | OpCode |
|---------------|-----------|---------|
| `transactions` | `TransactionHandler` | `OpCodeNewTransaction` |
| `transaction_items` | `TransactionItemHandler` | `OpCodeNewTransactionItems` |
| `menu_items` | `MenuItemHandler` | `OpCodeNewMenuItems` |
| `stores` | `StoreHandler` | `OpCodeNewStores` |
| `users` | `UserHandler` | `OpCodeNewUsers` |

### 4. Manejadores de Tabla (handlers.go)

Define manejadores específicos para cada tipo de datos del sistema.

#### Interfaz TableRowHandler
```go
type TableRowHandler interface {
    ProcessRecord(record []string) (map[string]string, error)
    GetExpectedFields() int
}
```

#### Manejadores Implementados

##### MenuItemHandler
Procesa datos de elementos del menú.

**Campos esperados (7):**
- `product_id` - ID del producto
- `name` - Nombre del producto
- `category` - Categoría
- `price` - Precio
- `is_seasonal` - Es estacional
- `available_from` - Disponible desde
- `available_to` - Disponible hasta

##### StoreHandler
Procesa datos de tiendas.

**Campos esperados (8):**
- `store_id` - ID de la tienda
- `store_name` - Nombre de la tienda
- `street` - Calle
- `postal_code` - Código postal
- `city` - Ciudad
- `state` - Estado/Provincia
- `latitude` - Latitud
- `longitude` - Longitud

##### TransactionHandler
Procesa datos de transacciones.

**Campos esperados (8):**
- `transaction_id` - ID de la transacción
- `store_id` - ID de la tienda
- `payment_method_id` - ID del método de pago
- `user_id` - ID del usuario
- `original_amount` - Monto original
- `discount_applied` - Descuento aplicado
- `final_amount` - Monto final
- `created_at` - Fecha de creación

##### TransactionItemHandler
Procesa datos de elementos de transacciones.

**Campos esperados (6):**
- `transaction_id` - ID de la transacción
- `item_id` - ID del elemento
- `quantity` - Cantidad
- `unit_price` - Precio unitario
- `subtotal` - Subtotal
- `created_at` - Fecha de creación

##### UserHandler
Procesa datos de usuarios.

**Campos esperados (4):**
- `user_id` - ID del usuario
- `gender` - Género
- `birthdate` - Fecha de nacimiento
- `registered_at` - Fecha de registro

## Flujo de Operación

### 1. Procesamiento de Directorio Completo
```go
// Crear procesador de archivos
fp := NewFileProcessor("cliente1", logger)

// Factory function para crear procesadores de lotes
processorFactory := func(handler TableRowHandler, opCode byte) *BatchProcessor {
    return NewBatchProcessor(conn, handler, opCode, 1000, "cliente1", logger)
}

// Procesar todas las tablas
ctx := context.Background()
err := fp.ProcessAllTables(ctx, processorFactory, "/datos")
```

### 2. Procesamiento de Tipo de Tabla Específico
```go
// Procesar solo transacciones
err := fp.ProcessTableType(ctx, "/datos", "transactions", processorFactory)
```

### 3. Procesamiento de Archivo Individual
```go
// Obtener manejador para el tipo
tth := NewTableTypeHandler("cliente1", logger)
handler, opCode, err := tth.GetHandlerAndOpCode("menu_items")

// Crear procesador de lotes
processor := NewBatchProcessor(conn, handler, opCode, 500, "cliente1", logger)

// Procesar archivo específico
err = fp.ProcessFile(ctx, "/datos/menu_items", "items.csv", processor)
```

### 4. Procesamiento por Lotes Manual
```go
// Configurar lector CSV
file, _ := os.Open("datos.csv")
reader := csv.NewReader(file)
reader.Read() // Saltar encabezado

// Crear y usar procesador de lotes
processor := NewBatchProcessor(conn, handler, opCode, 1000, "cliente1", logger)
err := processor.BuildAndSendBatches(ctx, reader, 1)
```

## Manejo de Errores

### Tipos de Error

1. **Errores de Archivo**
   - Archivo no encontrado
   - Permisos insuficientes
   - Formato CSV inválido

2. **Errores de Validación**
   - Número de campos incorrecto
   - Datos malformados
   - Tipos de datos inválidos

3. **Errores de Red**
   - Conexión perdida durante envío
   - Timeout de conexión
   - Errores de serialización

4. **Errores de Contexto**
   - Cancelación por timeout
   - Cancelación manual

### Estrategias de Recuperación

1. **Errores Críticos**: Errores de conexión terminan el procesamiento
2. **Errores de Archivo**: Se registran pero no detienen el procesamiento de otros archivos
3. **Errores de Validación**: Se registran y se omite la fila problemática
4. **Cancelación**: Se envía lote parcial antes de terminar

## Configuración y Parámetros

### Configuración CSV
- **Delimitador**: Coma (`,`)
- **Encabezados**: Primera fila omitida automáticamente
- **Campos variables**: Soportado (`FieldsPerRecord = -1`)

### Configuración de Lotes
- **Límite de filas**: Configurable por procesador
- **Límite de tamaño**: 1MB máximo (definido en protocolo)
- **Numeración**: Automática e incremental

### Configuración de Procesamiento
- **Delay entre archivos**: 1 segundo
- **Procesamiento asíncrono**: Gorrutinas para I/O
- **Soporte de contexto**: Cancelación y timeouts

## Logging y Monitoreo

### Eventos Registrados

1. **Archivo**
   - Inicio y finalización de procesamiento
   - Errores de apertura y lectura
   - Progreso del procesamiento

2. **Tabla**
   - Inicio de procesamiento por tipo
   - Tipos no soportados omitidos
   - Errores de directorio

3. **Lotes**
   - Construcción y envío de lotes
   - Errores de serialización
   - Estados de finalización

### Formato de Logs

Los logs siguen un formato estructurado:
- `action`: Acción realizada
- `result`: Resultado de la operación
- `file`: Archivo procesado
- `table_type`: Tipo de tabla
- `client_id`: Identificador del cliente
- `error`: Detalles del error (cuando aplica)

## Consideraciones de Rendimiento

### Optimizaciones Implementadas

1. **Streaming**: Procesamiento línea por línea sin cargar archivos completos
2. **Lotes optimizados**: Agrupación eficiente respetando límites
3. **Procesamiento asíncrono**: Uso de gorrutinas para I/O
4. **Reutilización de buffers**: Buffers reutilizables para lotes

### Gestión de Memoria

1. **Buffers limitados**: Límite de 1MB por lote
2. **Liberación automática**: Buffers se resetean después de envío
3. **Streaming CSV**: No se almacenan archivos completos en memoria

### Concurrencia

1. **Gorrutinas dedicadas**: Una por archivo procesado
2. **Channels para sincronización**: Coordinación entre gorrutinas
3. **Context support**: Cancelación coordinada

## Dependencias

- `encoding/csv` - Procesamiento de archivos CSV
- `context` - Soporte de contexto y cancelación
- `net` - Conexiones de red TCP
- `os` - Operaciones del sistema de archivos
- `path/filepath` - Manipulación de rutas
- `time` - Manejo de tiempos y delays
- `github.com/op/go-logging` - Sistema de logging
- `client/protocol` - Definiciones del protocolo
- `client/common/network` - Utilidades de red

## Extensibilidad

### Agregar Nuevo Tipo de Tabla

1. **Crear manejador**:
```go
type NewTableHandler struct{}

func (n NewTableHandler) ProcessRecord(record []string) (map[string]string, error) {
    // Implementar lógica de conversión
}

func (n NewTableHandler) GetExpectedFields() int {
    return X // número de campos esperados
}
```

2. **Agregar a TableTypeHandler**:
```go
case "new_table_type":
    return NewTableHandler{}, protocol.OpCodeNewTableType, nil
```

3. **Definir OpCode** en `protocol_constants.go`

### Personalizar Procesamiento

- **Configuración CSV**: Modificar delimitadores y formato
- **Límites de lote**: Ajustar según rendimiento
- **Validación**: Agregar validaciones específicas por campo
- **Transformaciones**: Implementar transformaciones de datos

## Consideraciones de Seguridad

- **Validación de entrada**: Verificación de número de campos
- **Sanitización**: Manejo seguro de datos CSV
- **Limits**: Respeto de límites de memoria y red
- **Logging seguro**: No exposición de datos sensibles en logs