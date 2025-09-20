import pika
import threading
from abc import ABC, abstractmethod
import time

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    @abstractmethod
    def stop_consuming(self):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def delete(self):
        pass

# -------------------------------------------------------------------
# Implementación concreta para RabbitMQ
# -------------------------------------------------------------------

class RabbitMQBase(MessageMiddleware):
    """Clase base para manejar la conexión y el canal comunes."""
    def __init__(self, host):
        self._host = host
        self._connection = None
        self._channel = None
        self._consumer_tag = None
        self._consuming_thread = None
        self.queue_name = None  # Initialize queue_name
        self._connect()

    def _connect(self):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
            self._channel = self._connection.channel()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en '{self._host}'") from e

    def start_consuming(self, on_message_callback):
        if not self.queue_name:
            raise MessageMiddlewareMessageError("Queue name not set")
            
        if self._consuming_thread and self._consuming_thread.is_alive():
            return # Ya está consumiendo

        def callback_wrapper(ch, method, properties, body):
            # Decodificamos el mensaje antes de pasarlo al callback final
            on_message_callback(body.decode('utf-8'))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        try:
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name, 
                on_message_callback=callback_wrapper
            )
            
            # El consumo se ejecuta en un hilo para no bloquear el programa principal
            self._consuming_thread = threading.Thread(target=self._channel.start_consuming, daemon=True)
            self._consuming_thread.start()

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error al iniciar el consumidor") from e

    def stop_consuming(self):
        if not self._consuming_thread or not self._consuming_thread.is_alive():
            return
            
        try:
            if self._channel and self._channel.is_open and self._consumer_tag:
                self._channel.basic_cancel(self._consumer_tag)
                self._channel.stop_consuming()  # Stop the consuming loop
                # Damos tiempo al hilo para que termine
                self._consuming_thread.join(timeout=5.0)
            self._consumer_tag = None
            self._consuming_thread = None
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareDisconnectedError("Error al detener el consumidor") from e

    def close(self):
        try:
            self.stop_consuming() # Detener el consumidor si está activo
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error al cerrar la conexión") from e

class MessageMiddlewareQueue(RabbitMQBase):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        super().__init__(host)
        self._setup_queue()

    def _setup_queue(self):
        # durable=True asegura que la cola sobreviva a reinicios del broker
        self._channel.queue_declare(queue=self.queue_name, durable=True)
        # Esto asegura una distribución justa de tareas (Round-Robin)
        self._channel.basic_qos(prefetch_count=1)

    def send(self, message):
        try:
            # El exchange vacío ('') publica directamente a la cola especificada en routing_key
            self._channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2) # Mensajes persistentes
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error al enviar el mensaje") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error al eliminar la cola '{self.queue_name}'") from e


class MessageMiddlewareExchange(RabbitMQBase):
    def __init__(self, host, exchange_name, route_keys):
        self.exchange_name = exchange_name
        # Las route_keys se usan tanto para suscribir (bind) como para publicar
        self.route_keys = route_keys if isinstance(route_keys, list) else [route_keys]
        # Guardamos la routing key principal para el método send
        if not self.route_keys:
            raise ValueError("MessageMiddlewareExchange requires at least one route key for sending.")
        self.default_routing_key = self.route_keys[0]
        
        super().__init__(host)
        self._setup_exchange()

    def _setup_exchange(self):
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
        # Fix: Remove durable=True for exclusive queues (they're contradictory)
        result = self._channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for key in self.route_keys:
            self._channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=key
            )

    def send(self, message):
        """
        Envía un mensaje al exchange usando la primera routing_key 
        provista durante la inicialización.
        """
        try:
            self._channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.default_routing_key, # Usa la clave por defecto
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error al enviar el mensaje") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error al eliminar el exchange '{self.exchange_name}'") from e