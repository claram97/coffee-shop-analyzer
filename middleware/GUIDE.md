# Middleware Client HOW-TO

Esta implementación de middleware provee dos formas de comunicarse:

*   **Direct (Queue):** Comunicación 1 a 1. El mensaje se envía a una cola específica y un solo consumidor lo recibe.
*   **Topic-based (Exchange):** Para comunicación 1 a muchos (pub/sub). Un mensaje se envía a un intercambio con una clave de enrutamiento, y todos los consumidores suscritos a ese topic (clave de enrutamiento) recibirán el mensaje.

Para esto, `middleware_client.py` proporciona dos clases:
*   `MessageMiddlewareQueue` para comunicación directa.
*   `MessageMiddlewareExchange` para comunicación basada en topics.

## Requisitos

El cliente utiliza la biblioteca `pika`. Se instala usando pip:
```bash
pip install pika
```

## Usar `MessageMiddlewareQueue`

Queues para comunicación 1 a 1.

### Enviar mensajes

Para enviar mensajes a una queue llamada `my_queue`:

```python
from middleware_client import MessageMiddlewareQueue, MessageMiddlewareMessageError

try:
    # Connect to RabbitMQ on localhost and specify the queue name
    queue = MessageMiddlewareQueue(host='localhost', queue_name='my_queue')

    # Send a message (must be bytes)
    message = b'Hello, from sender!'
    queue.send(message)
    print("Message sent!")

except MessageMiddlewareMessageError as e:
    print(f"Error sending message: {e}")
finally:
    # It's important to close the connection
    if 'queue' in locals():
        queue.close()
```

### Receiving Messages

Para recibir mensajes de `my_queue`:

```python
import time
from middleware_client import MessageMiddlewareQueue, MessageMiddlewareMessageError

# This function will be called for each message received
def on_message(body):
    print(f"Received message: {body.decode()}")

try:
    # Connect to RabbitMQ on localhost and specify the queue name
    queue = MessageMiddlewareQueue(host='localhost', queue_name='my_queue')

    # Start consuming messages in a background thread
    queue.start_consuming(on_message_callback=on_message)
    print("Waiting for messages. To exit press CTRL+C")

    # Keep the main thread alive to continue receiving messages
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping consumer...")
except MessageMiddlewareMessageError as e:
    print(f"Error: {e}")
finally:
    if 'queue' in locals():
        queue.stop_consuming()
        queue.close()
```

## Usar `MessageMiddlewareExchange`

Use esto para escenarios de pub/sub donde un mensaje puede ser recibido por múltiples consumidores en función de los topics.

### Enviar (Publicar) Mensajes

Para publicar mensajes en un exchange llamado `my_exchange` con una clave de enrutamiento (topic) específica:

```python
from middleware_client import MessageMiddlewareExchange, MessageMiddlewareMessageError

try:
    # Connect to RabbitMQ and specify the exchange name and a default route key for sending
    exchange = MessageMiddlewareExchange(
        host='localhost',
        exchange_name='my_exchange',
        route_keys=['logs.info']
    )

    # Publish a message (must be bytes)
    message = b'This is an informational log.'
    exchange.send(message)
    print("Message published!")

except MessageMiddlewareMessageError as e:
    print(f"Error publishing message: {e}")
finally:
    if 'exchange' in locals():
        exchange.close()
```

### Recibir (Suscribirse a) Mensajes

Para suscribirse a mensajes de `my_exchange` para uno o más topics (`route_keys`):

```python
import time
from middleware_client import MessageMiddlewareExchange, MessageMiddlewareMessageError

def on_log_message(body):
    print(f"Log received: {body.decode()}")

try:
    # Connect and subscribe to 'logs.info' and 'logs.error'
    subscriber = MessageMiddlewareExchange(
        host='localhost',
        exchange_name='my_exchange',
        route_keys=['logs.info', 'logs.error']
    )

    # Start consuming messages
    subscriber.start_consuming(on_message_callback=on_log_message)
    print("Subscribed to logs. Waiting for messages...")

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping subscriber...")
except MessageMiddlewareMessageError as e:
    print(f"Error: {e}")
finally:
    if 'subscriber' in locals():
        subscriber.stop_consuming()
        subscriber.close()
```

## Manejo de Errores

El cliente de middleware define un conjunto de excepciones personalizadas:

- `MessageMiddlewareMessageError`: Error general durante el procesamiento de mensajes (envío o consumo).
- `MessageMiddlewareDisconnectedError`: Se genera cuando la conexión a RabbitMQ falla.
- `MessageMiddlewareCloseError`: Error durante el cierre de una conexión.
- `MessageMiddlewareDeleteError`: Error al intentar eliminar una cola o un exchange.

## Graceful Shutdown

Es importante cerrar correctamente las conexiones para evitar leaks. Las clases proporcionan métodos para esto:
- `stop_consuming()`: Detiene el hilo de consumidor en segundo plano.
- `close()`: Detiene el cliente y cierra todas las conexiones.

## Correr con Docker

Se puede usar Docker y Docker Compose para levantar RabbitMQ y correr los nodos en contenedores aislados.

### Configuración de Docker Compose

Ejemplo de un `docker-compose.yml`. Esto configura un contenedor de RabbitMQ y dos contenedores mas, un `productor` y  un `consumidor`, que se construyen a partir de sus propios Dockerfiles.

```yaml
services:
  rabbitmq:
    image: "rabbitmq:3.9-management-alpine"
    hostname: "rabbitmq"
    ports:
      - "5672:5672"      # Puerto para clientes
      - "15672:15672"    # Puerto para la interfaz de administración (http://localhost:15672)
    networks:
      - coffee-network

  productor:
    build:
      context: ./productor # Carpeta con un Dockerfile de este nodo
    container_name: productor
    depends_on:
      - rabbitmq
    environment:
      - RABBITMQ_HOST=rabbitmq # El hostname del servicio de rabbitmq
    networks:
      - coffee-network

  consumidor:
    build:
      context: ./consumidor # Carpeta con un Dockerfile de este nodo
    container_name: consumidor
    depends_on:
      - rabbitmq
    environment:
      - RABBITMQ_HOST=rabbitmq
    networks:
      - coffee-network

networks:
  coffee-network:
    driver: bridge
```
Todos los nodos deben tener pika instalado. Se puede usar un `Dockerfile` como el de abajo para cualquiera de los nodos.

```dockerfile
# Usar una imagen oficial de Python
FROM python:3.9-slim

# Definir el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de dependencias
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el cliente de middleware y el script principal
COPY middleware_client.py .
COPY main.py .

# Comando para correr la aplicación
CMD ["python", "main.py"]
```