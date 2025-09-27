# Documentación de Red - Analizador de Cafeterías

## Descripción General

Este documento describe la implementación de la capa de red del cliente Go en el sistema analizador de cafeterías. Esta capa maneja las conexiones TCP, el procesamiento de respuestas del servidor y el envío de mensajes de finalización.

## Estructura del Paquete

La implementación de red se encuentra en el directorio `client/common/network/` y consta de los siguientes archivos:

- `connection.go` - Maneja la lógica de conexión TCP con reintentos automáticos
- `response_handling.go` - Procesa las respuestas del servidor y maneja mensajes de finalización

## Componentes Principales

### 1. ConnectionManager (connection.go)

Gestiona la lógica de conexión TCP para el cliente, incluyendo establecimiento de conexión, reintentos y limpieza.

#### Estructura
```go
type ConnectionManager struct {
    serverAddress string        // Dirección del servidor (host:puerto)
    clientID      string        // Identificador único del cliente
    conn          net.Conn      // Conexión TCP activa
    log           *logging.Logger // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewConnectionManager(serverAddress, clientID string, logger *logging.Logger) *ConnectionManager`
Crea una nueva instancia del administrador de conexiones.

**Parámetros:**
- `serverAddress`: Dirección del servidor en formato "host:puerto"
- `clientID`: Identificador único del cliente
- `logger`: Instancia del logger para registro de eventos

##### `Connect(attempts int, retryInterval time.Duration) error`
Establece la conexión TCP con lógica de reintentos.

**Parámetros:**
- `attempts`: Número máximo de intentos de conexión
- `retryInterval`: Tiempo de espera entre reintentos

**Comportamiento:**
- Realiza múltiples intentos de conexión
- Registra cada intento y su resultado
- Espera el intervalo especificado entre reintentos
- Retorna error si todos los intentos fallan

##### `GetConnection() net.Conn`
Retorna la conexión TCP activa.

##### `Close()`
Cierra la conexión TCP de forma segura y realiza la limpieza de recursos.

#### Funciones Auxiliares

##### `IsConnectionError(err error) bool`
Verifica si un error indica una conexión rota o perdida.

**Tipos de errores detectados:**
- `net.ErrClosed` - Conexión cerrada
- `io.ErrClosedPipe` - Tubería cerrada
- `io.ErrUnexpectedEOF` - Fin de archivo inesperado
- `*net.OpError` - Errores de operación de red
- `*os.SyscallError` - Errores de llamadas al sistema (EPIPE, ECONNRESET, etc.)

##### Funciones de Logging Interno
- `logConnectionAttempt()` - Registra intentos de conexión
- `handleConnectionSuccess()` - Maneja conexiones exitosas
- `handleConnectionFailure()` - Maneja fallos de conexión
- `waitBeforeRetry()` - Gestiona esperas entre reintentos
- `logAllRetriesFailed()` - Registra cuando fallan todos los reintentos

### 2. ResponseHandler (response_handling.go)

Maneja la comunicación de respuestas del servidor en una gorrutina dedicada.

#### Estructura
```go
type ResponseHandler struct {
    conn     net.Conn        // Conexión TCP
    clientID string          // Identificador del cliente
    log      *logging.Logger // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewResponseHandler(conn net.Conn, clientID string, logger *logging.Logger) *ResponseHandler`
Crea una nueva instancia del manejador de respuestas.

##### `ReadResponses(readDone chan struct{})`
Consume las respuestas del servidor desde la conexión en una gorrutina dedicada.

**Comportamiento:**
- Ejecuta un bucle de lectura en gorrutina separada
- Registra resultados por mensaje
- Termina cuando ocurre un error de I/O (incluyendo EOF)
- Cierra el canal `readDone` al terminar

##### `WaitForResponses(ctx context.Context, readDone chan struct{})`
Espera a que se complete el procesamiento de respuestas con soporte de contexto.

**Comportamiento:**
- Si se cancela el contexto: establece deadline de lectura y espera finalización
- Si termina normalmente: realiza cierre ordenado de la escritura TCP

#### Funciones Auxiliares

##### `handleReadError(err error) bool`
Maneja diferentes tipos de errores de lectura.

**Tipos de errores:**
- `io.EOF` - Servidor cerró la conexión
- Errores de conexión - Conexión perdida
- Otros errores - Errores generales de I/O

##### `handleResponseMessage(msg interface{})`
Procesa un mensaje de respuesta recibido.

**Tipos de respuesta procesados:**
- `BatchRecvSuccessOpCode` - Lote procesado exitosamente
- `BatchRecvFailOpCode` - Fallo en procesamiento de lote

##### `responseReaderLoop(reader *bufio.Reader)`
Ejecuta el bucle principal de lectura de respuestas.

### 3. FinishedMessageSender (response_handling.go)

Maneja el envío del mensaje FINISHED al servidor.

#### Estructura
```go
type FinishedMessageSender struct {
    conn     net.Conn        // Conexión TCP
    clientID string          // Identificador del cliente
    log      *logging.Logger // Logger para registro de eventos
}
```

#### Funciones Principales

##### `NewFinishedMessageSender(conn net.Conn, clientID string, logger *logging.Logger) *FinishedMessageSender`
Crea una nueva instancia del enviador de mensajes finalizados.

##### `SendFinished()`
Envía el mensaje FINISHED con el ID numérico de la agencia.

**Comportamiento:**
- Convierte el `clientID` string a entero (agencyId)
- Crea y serializa el mensaje `protocol.Finished`
- Envía el mensaje por la conexión TCP
- Registra éxito o fallo de cada escritura

## Flujo de Operación

### 1. Establecimiento de Conexión
```go
// Crear administrador de conexión
cm := NewConnectionManager("servidor:8080", "cliente1", logger)

// Conectar con reintentos
err := cm.Connect(3, 5*time.Second) // 3 intentos, 5 seg entre reintentos
if err != nil {
    // Manejar error de conexión
}

// Obtener conexión
conn := cm.GetConnection()
```

### 2. Procesamiento de Respuestas
```go
// Crear manejador de respuestas  
rh := NewResponseHandler(conn, "cliente1", logger)

// Iniciar lectura de respuestas
readDone := make(chan struct{})
rh.ReadResponses(readDone)

// Realizar operaciones de envío de datos...

// Esperar finalización con timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
rh.WaitForResponses(ctx, readDone)
```

### 3. Finalización
```go
// Enviar mensaje de finalización
fms := NewFinishedMessageSender(conn, "cliente1", logger)
fms.SendFinished()

// Cerrar conexión
cm.Close()
```

## Manejo de Errores

### Tipos de Error

1. **Errores de Conexión**
   - Conexión rechazada por el servidor
   - Host inalcanzable
   - Timeout de conexión

2. **Errores de Red**
   - Conexión perdida durante operación
   - Pipe roto (EPIPE)
   - Reset de conexión (ECONNRESET)

3. **Errores de Protocolo**
   - Formato de mensaje inválido
   - Errores de serialización

### Estrategias de Recuperación

1. **Reintentos Automáticos**: El `ConnectionManager` realiza múltiples intentos con intervalo configurable
2. **Detección de Desconexión**: `IsConnectionError()` identifica conexiones perdidas
3. **Cierre Ordenado**: Uso de `CloseWrite()` para cierre TCP ordenado
4. **Timeouts**: Soporte de contexto para operaciones con timeout

## Logging y Monitoreo

### Eventos Registrados

1. **Conexión**
   - Intentos de conexión
   - Éxito/fallo de conexión
   - Reintentos y esperas

2. **Respuestas**
   - Mensajes recibidos y su tipo
   - Errores de lectura
   - Cierre de conexión por servidor

3. **Finalización**
   - Envío de mensaje FINISHED
   - Conversión de clientID a agencyID
   - Errores de serialización/envío

### Formato de Logs

Los logs siguen un formato estructurado con campos clave:
- `action`: Acción realizada
- `result`: Resultado de la operación
- `client_id`: Identificador del cliente
- `error`: Detalles del error (cuando aplica)

## Consideraciones de Rendimiento

### Concurrencia
- **Gorrutinas dedicadas** para lectura de respuestas
- **Operaciones no bloqueantes** para I/O de red
- **Canales para sincronización** entre gorrutinas

### Gestión de Recursos
- **Cierre automático** de conexiones en caso de error
- **Limpieza de recursos** en destructor
- **Reutilización** de conexiones establecidas

### Timeouts y Deadlines
- **Context support** para operaciones con timeout
- **Read deadlines** para evitar bloqueos indefinidos
- **Graceful shutdown** con cierre ordenado de escritura

## Dependencias

- `net` - Operaciones de red TCP
- `bufio` - Buffer de I/O para lectura eficiente
- `context` - Soporte de contexto para timeouts
- `time` - Manejo de tiempos y timeouts
- `github.com/op/go-logging` - Sistema de logging
- `client/protocol` - Definiciones del protocolo de comunicación

## Consideraciones de Seguridad

- **Validación de errores** antes de procesar respuestas
- **Límites de timeout** para evitar ataques de denegación de servicio
- **Logging seguro** sin exposición de datos sensibles
- **Manejo seguro** de conversiones de tipo

## Notas de Implementación

- **Thread-safe**: Las operaciones de red son seguras entre gorrutinas
- **Error handling robusto**: Manejo exhaustivo de errores de red y protocolo
- **Logging estructurado**: Facilita monitoreo y debugging
- **Configuración flexible**: Reintentos y timeouts configurables
- **Desacoplamiento**: Separación clara de responsabilidades entre componentes