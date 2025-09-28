# Documentación del Protocolo - Cliente

## Descripción General

Este documento describe el protocolo de comunicación utilizado entre el cliente Go y el orquestador Python en el sistema analizador de cafeterías. El protocolo es binario, basado en frames y utiliza orden de bytes little-endian para valores de múltiples bytes.

## Estructura del Paquete

La implementación del protocolo se encuentra en el directorio `client/protocol/` y consiste de los siguientes archivos:

- `protocol_constants.go` - Define códigos de operación, constantes de estado de batch y límites del protocolo
- `protocol_interfaces.go` - Define interfaces principales y tipos de error
- `messages.go` - Implementa mensajes del protocolo y análisis de mensajes
- `batch_operations.go` - Proporciona funcionalidad de gestión de batches y manejo de filas
- `batch_writer.go` - Maneja la serialización y el enmarcado de batches
- `serialization.go` - Implementa funciones de serialización de bajo nivel

## Constantes del Protocolo

### Códigos de Operación (OpCodes)

| OpCode | Valor | Dirección | Descripción |
|--------|-------|-----------|-------------|
| `BatchRecvSuccessOpCode` | 1 | Servidor→Cliente | Batch procesado exitosamente |
| `BatchRecvFailOpCode` | 2 | Servidor→Cliente | Falló el procesamiento del batch |
| `OpCodeFinished` | 3 | Cliente→Servidor | Cliente terminó de enviar todos los batches |
| `OpCodeNewMenuItems` | 4 | Cliente→Servidor | Batch contiene datos de elementos del menú |
| `OpCodeNewStores` | 5 | Cliente→Servidor | Batch contiene datos de tiendas |
| `OpCodeNewTransactionItems` | 6 | Cliente→Servidor | Batch contiene datos de elementos de transacción |
| `OpCodeNewTransaction` | 7 | Cliente→Servidor | Batch contiene datos de transacciones |
| `OpCodeNewUsers` | 8 | Cliente→Servidor | Batch contiene datos de usuarios |
| `OpCodeEOF` | 9 | Cliente→Servidor | **Fin de tabla específica** - Indica que terminó un tipo de datos completo |

### Estado del Batch

| Estado | Valor | Descripción |
|--------|-------|-------------|
| `BatchContinue` | 0 | Más batches seguirán para este archivo |
| `BatchEOF` | 1 | **Último batch del archivo actual** - También usado en mensajes EOF de tabla |
| `BatchCancel` | 2 | Batch enviado debido a cancelación |

**Nota importante:** `BatchEOF` tiene **doble propósito**:
1. **A nivel archivo**: Marca el último batch de un archivo CSV individual  
2. **A nivel tabla**: Se usa en el status del mensaje `OpCodeEOF` para indicar fin de tabla completa

### Límites del Protocolo

- **Tamaño Máximo de Batch**: 1MB (1,048,576 bytes) - `MaxBatchSizeBytes`
- **Overhead del Header**: 18 bytes por mensaje - `HeaderOverhead`
  - 1 byte (opcode) + 4 bytes (length) + 4 bytes (nLines) + 8 bytes (batchNumber) + 1 byte (status)

### Nuevas Características del Protocolo

- **EOF Automático**: Sistema de señalización de fin de tabla implementado
- **Comentarios Bilingües**: Constantes documentadas en español para mejor comprensión
- **Gestión Avanzada de Estados**: Diferenciación clara entre EOF de archivo vs EOF de tabla

## Formato de Mensajes

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

## Interfaces Principales

### Interfaz Message
```go
type Message interface {
    GetOpCode() byte
    GetLength() int32
}
```

### Interfaz Writeable
```go
type Writeable interface {
    WriteTo(out io.Writer) (int64, error)
}
```

### Interfaz Readable
```go
type Readable interface {
    readFrom(reader *bufio.Reader) error
    Message
}
```

## Mensajes del Protocolo

### 1. Mensaje Finished (Cliente→Servidor)

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

### 2. BatchRecvSuccess (Servidor→Cliente)

Confirmación de que un batch fue procesado exitosamente.

**Estructura:**
- OpCode: `BatchRecvSuccessOpCode` (1)
- Length: 0 bytes
- Body: (vacío)

### 3. BatchRecvFail (Servidor→Cliente)

Confirmación negativa de que falló el procesamiento de un batch.

**Estructura:**
- OpCode: `BatchRecvFailOpCode` (2)  
- Length: 0 bytes
- Body: (vacío)

### 4. **NUEVO:** Mensaje EOF (Cliente→Servidor)

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

**Ejemplo de uso automático:**
```go
// El sistema envía automáticamente EOF después de procesar:
// menu_items/file1.csv → batches 1-5
// menu_items/file2.csv → batches 6-8  
// menu_items/file3.csv → batches 9-12
// → EOF automático con table_type="menu_items"
```

**Beneficios:**
- **Sincronización precisa** del procesamiento downstream
- **Demarcación clara** entre diferentes tipos de datos
- **Triggers automáticos** para análisis por tabla
- **Compatibilidad** con arquitecturas de microservicios

## Operaciones de Batch

### Agregando Filas a Batches

La función `AddRowToBatch` gestiona los límites de tamaño de batch y el vaciado automático:

```go
func AddRowToBatch(
    row map[string]string,
    to *bytes.Buffer,
    finalOutput io.Writer,
    counter *int32,
    batchLimit int32,
    opCode byte,
    batchNumber *int64
) error
```

**Comportamiento:**
- Intenta agregar una fila al batch actual
- Si agregar la fila excedería `MaxBatchSizeBytes` o `batchLimit`, automáticamente vacía el batch actual e inicia uno nuevo
- Gestiona la numeración de batches automáticamente
- Retorna error en fallas de serialización o I/O

### Vaciado de Batch

La función `FlushBatch` serializa y envía un batch completo:

```go
func FlushBatch(
    batch *bytes.Buffer,
    out io.Writer,
    counter int32,
    opCode byte,
    batchNumber int64,
    batchStatus byte
) error
```

## Manejo de Errores

### ProtocolError

Tipo de error personalizado para errores relacionados con el protocolo:

```go
type ProtocolError struct {
    Msg    string
    Opcode byte
}
```

**Escenarios de Error Comunes:**
- OpCode inválido en mensajes entrantes
- Discrepancia en longitud del cuerpo
- Fallas de serialización
- Errores de I/O de red

## Ejemplos de Uso

### Enviando un Batch de Elementos del Menú

```go
var batch bytes.Buffer
var counter int32
var batchNumber int64 = 1

// Agregar filas al batch
for _, menuItem := range menuItems {
    row := map[string]string{
        "id":    menuItem.ID,
        "name":  menuItem.Name,
        "price": menuItem.Price,
    }
    
    err := AddRowToBatch(row, &batch, writer, &counter, 1000, OpCodeNewMenuItems, &batchNumber)
    if err != nil {
        // manejar error
    }
}

// Vaciar batch final con estado EOF
if counter > 0 {
    err := FlushBatch(&batch, writer, counter, OpCodeNewMenuItems, batchNumber, BatchEOF)
}
```

### Leyendo Respuestas del Servidor

```go
reader := bufio.NewReader(connection)

for {
    msg, err := ReadMessage(reader)
    if err != nil {
        // manejar error
        break
    }
    
    switch msg.GetOpCode() {
    case BatchRecvSuccessOpCode:
        // Batch procesado exitosamente
        fmt.Println("Batch confirmado")
    case BatchRecvFailOpCode:
        // Falló el procesamiento del batch
        fmt.Println("Batch falló")
    }
}
```

## Flujo del Protocolo

### Flujo Básico (por tabla)
1. **Establecimiento de Conexión**: El cliente establece conexión TCP al servidor
2. **Procesamiento por Tabla**: Para cada tipo de datos (menu_items/, stores/, etc.):
   - **Envío de Batches**: Cliente envía batches usando OpCodes apropiados (4-8)
   - **Confirmaciones**: Servidor responde con éxito/falla para cada batch
   - **EOF de Archivos**: Cada archivo termina con `BatchEOF` en su último batch
   - **EOF de Tabla**: Al completar todos los archivos, se envía `OpCodeEOF` automáticamente
3. **Completación Global**: Cliente envía mensaje `Finished` cuando termina todas las tablas
4. **Terminación de Conexión**: Se cierra la conexión

### Flujo Detallado con EOF
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

## Mejores Prácticas con EOF

### Procesamiento por Tabla
```go
// ✅ CORRECTO: Una instancia de BatchProcessor por tabla
func ProcessTableType(tableType string) error {
    processor := NewBatchProcessor(conn, handler, opcode, ...)
    
    // Procesar todos los archivos de la tabla
    for _, file := range csvFiles {
        processor.ProcessFile(file)  // Cada archivo termina con BatchEOF
    }
    
    // EOF automático se envía al completar la tabla
    return sendEOFMessage(processor, tableType)
}
```

### Monitoreo de Estado
```go
// Logging recomendado para EOF
log.Infof("action: sent_eof | result: success | table_type: %s", tableType)
log.Infof("action: eof_received | table_type: %s | batch_number: %d", tableType, lastBatch)
```

### Manejo de Errores EOF
```go
if err := sendEOFMessage(processor, tableType); err != nil {
    if network.IsConnectionError(err) {
        return fmt.Errorf("connection lost during EOF: %w", err)
    }
    return fmt.Errorf("failed to send EOF for %s: %w", tableType, err)
}
```

## Seguridad de Hilos

La implementación del protocolo **no es thread-safe**. Si múltiples goroutines necesitan enviar datos simultáneamente, se requiere sincronización externa.

**Recomendación**: Use una goroutine por tabla con sincronización en el nivel de conexión TCP.