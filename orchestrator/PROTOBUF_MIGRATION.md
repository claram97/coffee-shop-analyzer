# Orchestrator - Protobuf Migration

## Cambios Realizados

### ✅ Archivos Nuevos

1. **`orchestrator/protos/`** - Archivos Protocol Buffers
   - `envelope.proto` - Mensaje contenedor principal
   - `table_data.proto` - Definición de datos de tablas
   - `query_request.proto` - Solicitudes de queries
   - `eof_message.proto` - Mensajes EOF

2. **`orchestrator/common/protobuf_handler.py`** - Manejadores de protobuf
   - `ProtobufMessageReader` - Lee mensajes Envelope desde socket
   - `ProtobufMessageWriter` - Escribe mensajes Envelope a socket
   - `ProtobufMessageHandler` - Procesa envelopes por tipo

3. **`orchestrator/requirements.txt`** - Dependencias actualizadas
   ```
   pika>=1.3.0
   protobuf>=4.25.0
   grpcio-tools>=1.60.0
   ```

4. **`orchestrator/generate_protos.sh`** - Script para generar código Python desde .proto

### ✅ Archivos Modificados

1. **`orchestrator/app/net.py`** - Orquestador principal
   - Usa `ProtobufMessageHandler` en lugar de `MessageHandler` custom
   - Lee mensajes con `ProtobufMessageReader`
   - Escribe respuestas con `ProtobufMessageWriter`
   - Procesa 4 tipos de mensajes:
     - `TABLE_DATA` - Datos de tablas
     - `EOF` - Fin de tabla
     - `QUERY_REQUEST` - Solicitud de query
     - `FINISHED` - Cliente terminó

## Compilación de Protobuf

### Opción 1: Local (si tienes Python con pip)
```bash
cd orchestrator
pip install -r requirements.txt
python -m grpc_tools.protoc \
    -I./protos \
    --python_out=./protos \
    --pyi_out=./protos \
    protos/*.proto
```

### Opción 2: En Docker
```bash
cd orchestrator
docker build -t orchestrator:latest .
docker run --rm -v $(pwd)/protos:/protos orchestrator:latest \
    python -m grpc_tools.protoc \
    -I/protos \
    --python_out=/protos \
    --pyi_out=/protos \
    /protos/*.proto
```

### Opción 3: Usar el script
```bash
cd orchestrator
./generate_protos.sh
```

## Archivos Generados

Después de compilar, deberías tener:
```
orchestrator/protos/
├── __init__.py
├── envelope.proto
├── envelope_pb2.py      ← Generado
├── envelope_pb2.pyi     ← Generado
├── table_data.proto
├── table_data_pb2.py    ← Generado
├── table_data_pb2.pyi   ← Generado
├── query_request.proto
├── query_request_pb2.py ← Generado
├── query_request_pb2.pyi ← Generado
├── eof_message.proto
├── eof_message_pb2.py   ← Generado
└── eof_message_pb2.pyi  ← Generado
```

## Flujo de Comunicación

### Cliente Go → Orquestador Python

```
1. Cliente lee CSV
   ↓
2. Crea TableData protobuf
   ↓
3. Envuelve en Envelope(TABLE_DATA)
   ↓
4. Serializa: [4 bytes tamaño][N bytes protobuf]
   ↓
5. Envía a orquestador

6. Orquestador lee con ProtobufMessageReader
   ↓
7. Procesa según Envelope.type
   ↓
8. Responde con Envelope(BATCH_RECV_SUCCESS)
```

### Tipos de Mensajes

| Envelope Type | Dirección | Payload | Descripción |
|---------------|-----------|---------|-------------|
| `TABLE_DATA` | Cliente → Servidor | `TableData` | Batch de datos de una tabla |
| `EOF` | Cliente → Servidor | `EOFMessage` | Fin de transmisión de tabla |
| `QUERY_REQUEST` | Cliente → Servidor | `QueryRequest` | Solicitud de query |
| `FINISHED` | Cliente → Servidor | None | Cliente terminó todo |
| `BATCH_RECV_SUCCESS` | Servidor → Cliente | None | ACK exitoso |
| `BATCH_RECV_FAIL` | Servidor → Cliente | None | NACK (error) |

## Testing

Para probar la integración:

```bash
# 1. Iniciar orquestador
cd orchestrator
python app/main.py

# 2. Enviar datos desde cliente Go
cd client
./client
```

## Troubleshooting

### Error: "No module named 'protos.envelope_pb2'"
**Solución**: Compilar los archivos .proto
```bash
cd orchestrator
./generate_protos.sh
```

### Error: "No module named 'grpc_tools'"
**Solución**: Instalar dependencias
```bash
pip install -r requirements.txt
```

### Error: Imports no resuelven en los *_pb2.py
**Solución**: Los imports en protobuf pueden ser relativos. Si hay problemas, agregar al `PYTHONPATH`:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

## Compatibilidad

- ✅ **Cliente Go**: Usa protobuf generado desde los mismos .proto
- ✅ **Filter/Joiner Python**: Recibirá mensajes protobuf serializados via RabbitMQ
- ⚠️ **Tests antiguos**: Pueden necesitar actualización para usar protobuf

## Próximos Pasos

1. Actualizar Filter y Joiner para usar protobuf
2. Actualizar tests para protocolo protobuf
3. Actualizar Docker Compose para incluir generación de protos
4. Documentar schema protobuf en detalle
