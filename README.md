# TP0: Docker + Comunicaciones + Concurrencia

En el presente repositorio se provee un esqueleto básico de cliente/servidor, en donde todas las dependencias del mismo se encuentran encapsuladas en containers. Los alumnos deberán resolver una guía de ejercicios incrementales, teniendo en cuenta las condiciones de entrega descritas al final de este enunciado.

El cliente (Golang) y el servidor (Python) fueron desarrollados en diferentes lenguajes simplemente para mostrar cómo dos lenguajes de programación pueden convivir en el mismo proyecto con la ayuda de containers, en este caso utilizando [Docker Compose](https://docs.docker.com/compose/).

## Instrucciones de uso

El repositorio cuenta con un **Makefile** que incluye distintos comandos en forma de targets. Los targets se ejecutan mediante la invocación de: **make \<target\>**. Los target imprescindibles para iniciar y detener el sistema son **docker-compose-up** y **docker-compose-down**, siendo los restantes targets de utilidad para el proceso de depuración.

Los targets disponibles son:

| target                | accion                                                                                                                                                                                                                                                                                                                                                                |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `docker-compose-up`   | Inicializa el ambiente de desarrollo. Construye las imágenes del cliente y el servidor, inicializa los recursos a utilizar (volúmenes, redes, etc) e inicia los propios containers.                                                                                                                                                                                   |
| `docker-compose-down` | Ejecuta `docker-compose stop` para detener los containers asociados al compose y luego `docker-compose down` para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene de versiones de desarrollo y recursos sin liberar. |
| `docker-compose-logs` | Permite ver los logs actuales del proyecto. Acompañar con `grep` para lograr ver mensajes de una aplicación específica dentro del compose.                                                                                                                                                                                                                            |
| `docker-image`        | Construye las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por **docker-compose-up**, por lo cual se lo puede utilizar para probar nuevos cambios en las imágenes antes de arrancar el proyecto.                                                                                                                       |
| `build`               | Compila la aplicación cliente para ejecución en el _host_ en lugar de en Docker. De este modo la compilación es mucho más veloz, pero requiere contar con todo el entorno de Golang y Python instalados en la máquina _host_.                                                                                                                                         |

### Servidor

Se trata de un "echo server", en donde los mensajes recibidos por el cliente se responden inmediatamente y sin alterar.

Se ejecutan en bucle las siguientes etapas:

1. Servidor acepta una nueva conexión.
2. Servidor recibe mensaje del cliente y procede a responder el mismo.
3. Servidor desconecta al cliente.
4. Servidor retorna al paso 1.

### Cliente

se conecta reiteradas veces al servidor y envía mensajes de la siguiente forma:

1. Cliente se conecta al servidor.
2. Cliente genera mensaje incremental.
3. Cliente envía mensaje al servidor y espera mensaje de respuesta.
4. Servidor responde al mensaje.
5. Servidor desconecta al cliente.
6. Cliente verifica si aún debe enviar un mensaje y si es así, vuelve al paso 2.

### Ejemplo

Al ejecutar el comando `make docker-compose-up` y luego `make docker-compose-logs`, se observan los siguientes logs:

```
client1  | 2024-08-21 22:11:15 INFO     action: config | result: success | client_id: 1 | server_address: server:12345 | loop_amount: 5 | loop_period: 5s | log_level: DEBUG
client1  | 2024-08-21 22:11:15 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°1
server   | 2024-08-21 22:11:14 DEBUG    action: config | result: success | port: 12345 | listen_backlog: 5 | logging_level: DEBUG
server   | 2024-08-21 22:11:14 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:15 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:15 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°1
server   | 2024-08-21 22:11:15 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:20 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:20 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°2
server   | 2024-08-21 22:11:20 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:20 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°2
server   | 2024-08-21 22:11:25 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:25 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°3
client1  | 2024-08-21 22:11:25 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°3
server   | 2024-08-21 22:11:25 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:30 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:30 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°4
server   | 2024-08-21 22:11:30 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:30 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°4
server   | 2024-08-21 22:11:35 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:35 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°5
client1  | 2024-08-21 22:11:35 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°5
server   | 2024-08-21 22:11:35 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:40 INFO     action: loop_finished | result: success | client_id: 1
client1 exited with code 0
```

## Parte 1: Introducción a Docker

En esta primera parte del trabajo práctico se plantean una serie de ejercicios que sirven para introducir las herramientas básicas de Docker que se utilizarán a lo largo de la materia. El entendimiento de las mismas será crucial para el desarrollo de los próximos TPs.

### Ejercicio N°1:

Definir un script de bash `generar-compose.sh` que permita crear una definición de Docker Compose con una cantidad configurable de clientes. El nombre de los containers deberá seguir el formato propuesto: client1, client2, client3, etc.

El script deberá ubicarse en la raíz del proyecto y recibirá por parámetro el nombre del archivo de salida y la cantidad de clientes esperados:

`./generar-compose.sh docker-compose-dev.yaml 5`

Considerar que en el contenido del script pueden invocar un subscript de Go o Python:

```
#!/bin/bash
echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"
python3 mi-generador.py $1 $2
```

En el archivo de Docker Compose de salida se pueden definir volúmenes, variables de entorno y redes con libertad, pero recordar actualizar este script cuando se modifiquen tales definiciones en los sucesivos ejercicios.

### Ejercicio N°2:

Modificar el cliente y el servidor para lograr que realizar cambios en el archivo de configuración no requiera reconstruír las imágenes de Docker para que los mismos sean efectivos. La configuración a través del archivo correspondiente (`config.ini` y `config.yaml`, dependiendo de la aplicación) debe ser inyectada en el container y persistida por fuera de la imagen (hint: `docker volumes`).

### Ejercicio N°3:

Crear un script de bash `validar-echo-server.sh` que permita verificar el correcto funcionamiento del servidor utilizando el comando `netcat` para interactuar con el mismo. Dado que el servidor es un echo server, se debe enviar un mensaje al servidor y esperar recibir el mismo mensaje enviado.

En caso de que la validación sea exitosa imprimir: `action: test_echo_server | result: success`, de lo contrario imprimir:`action: test_echo_server | result: fail`.

El script deberá ubicarse en la raíz del proyecto. Netcat no debe ser instalado en la máquina _host_ y no se pueden exponer puertos del servidor para realizar la comunicación (hint: `docker network`). `

### Ejercicio N°4:

Modificar servidor y cliente para que ambos sistemas terminen de forma _graceful_ al recibir la signal SIGTERM. Terminar la aplicación de forma _graceful_ implica que todos los _file descriptors_ (entre los que se encuentran archivos, sockets, threads y procesos) deben cerrarse correctamente antes que el thread de la aplicación principal muera. Loguear mensajes en el cierre de cada recurso (hint: Verificar que hace el flag `-t` utilizado en el comando `docker compose down`).

## Parte 2: Repaso de Comunicaciones

Las secciones de repaso del trabajo práctico plantean un caso de uso denominado **Lotería Nacional**. Para la resolución de las mismas deberá utilizarse como base el código fuente provisto en la primera parte, con las modificaciones agregadas en el ejercicio 4.

### Ejercicio N°5:

Modificar la lógica de negocio tanto de los clientes como del servidor para nuestro nuevo caso de uso.

#### Cliente

Emulará a una _agencia de quiniela_ que participa del proyecto. Existen 5 agencias. Deberán recibir como variables de entorno los campos que representan la apuesta de una persona: nombre, apellido, DNI, nacimiento, numero apostado (en adelante 'número'). Ej.: `NOMBRE=Santiago Lionel`, `APELLIDO=Lorca`, `DOCUMENTO=30904465`, `NACIMIENTO=1999-03-17` y `NUMERO=7574` respectivamente.

Los campos deben enviarse al servidor para dejar registro de la apuesta. Al recibir la confirmación del servidor se debe imprimir por log: `action: apuesta_enviada | result: success | dni: ${DNI} | numero: ${NUMERO}`.

#### Servidor

Emulará a la _central de Lotería Nacional_. Deberá recibir los campos de la cada apuesta desde los clientes y almacenar la información mediante la función `store_bet(...)` para control futuro de ganadores. La función `store_bet(...)` es provista por la cátedra y no podrá ser modificada por el alumno.
Al persistir se debe imprimir por log: `action: apuesta_almacenada | result: success | dni: ${DNI} | numero: ${NUMERO}`.

#### Comunicación:

Se deberá implementar un módulo de comunicación entre el cliente y el servidor donde se maneje el envío y la recepción de los paquetes, el cual se espera que contemple:

- Definición de un protocolo para el envío de los mensajes.
- Serialización de los datos.
- Correcta separación de responsabilidades entre modelo de dominio y capa de comunicación.
- Correcto empleo de sockets, incluyendo manejo de errores y evitando los fenómenos conocidos como [_short read y short write_](https://cs61.seas.harvard.edu/site/2018/FileDescriptors/).

### Ejercicio N°6:

Modificar los clientes para que envíen varias apuestas a la vez (modalidad conocida como procesamiento por _chunks_ o _batchs_).
Los _batchs_ permiten que el cliente registre varias apuestas en una misma consulta, acortando tiempos de transmisión y procesamiento.

La información de cada agencia será simulada por la ingesta de su archivo numerado correspondiente, provisto por la cátedra dentro de `.data/datasets.zip`.
Los archivos deberán ser inyectados en los containers correspondientes y persistido por fuera de la imagen (hint: `docker volumes`), manteniendo la convencion de que el cliente N utilizara el archivo de apuestas `.data/agency-{N}.csv` .

En el servidor, si todas las apuestas del _batch_ fueron procesadas correctamente, imprimir por log: `action: apuesta_recibida | result: success | cantidad: ${CANTIDAD_DE_APUESTAS}`. En caso de detectar un error con alguna de las apuestas, debe responder con un código de error a elección e imprimir: `action: apuesta_recibida | result: fail | cantidad: ${CANTIDAD_DE_APUESTAS}`.

La cantidad máxima de apuestas dentro de cada _batch_ debe ser configurable desde config.yaml. Respetar la clave `batch: maxAmount`, pero modificar el valor por defecto de modo tal que los paquetes no excedan los 8kB.

Por su parte, el servidor deberá responder con éxito solamente si todas las apuestas del _batch_ fueron procesadas correctamente.

### Ejercicio N°7:

Modificar los clientes para que notifiquen al servidor al finalizar con el envío de todas las apuestas y así proceder con el sorteo.
Inmediatamente después de la notificacion, los clientes consultarán la lista de ganadores del sorteo correspondientes a su agencia.
Una vez el cliente obtenga los resultados, deberá imprimir por log: `action: consulta_ganadores | result: success | cant_ganadores: ${CANT}`.

El servidor deberá esperar la notificación de las 5 agencias para considerar que se realizó el sorteo e imprimir por log: `action: sorteo | result: success`.
Luego de este evento, podrá verificar cada apuesta con las funciones `load_bets(...)` y `has_won(...)` y retornar los DNI de los ganadores de la agencia en cuestión. Antes del sorteo no se podrán responder consultas por la lista de ganadores con información parcial.

Las funciones `load_bets(...)` y `has_won(...)` son provistas por la cátedra y no podrán ser modificadas por el alumno.

No es correcto realizar un broadcast de todos los ganadores hacia todas las agencias, se espera que se informen los DNIs ganadores que correspondan a cada una de ellas.

## Parte 3: Repaso de Concurrencia

En este ejercicio es importante considerar los mecanismos de sincronización a utilizar para el correcto funcionamiento de la persistencia.

### Ejercicio N°8:

Modificar el servidor para que permita aceptar conexiones y procesar mensajes en paralelo. En caso de que el alumno implemente el servidor en Python utilizando _multithreading_, deberán tenerse en cuenta las [limitaciones propias del lenguaje](https://wiki.python.org/moin/GlobalInterpreterLock).

## Condiciones de Entrega

Se espera que los alumnos realicen un _fork_ del presente repositorio para el desarrollo de los ejercicios y que aprovechen el esqueleto provisto tanto (o tan poco) como consideren necesario.

Cada ejercicio deberá resolverse en una rama independiente con nombres siguiendo el formato `ej${Nro de ejercicio}`. Se permite agregar commits en cualquier órden, así como crear una rama a partir de otra, pero al momento de la entrega deberán existir 8 ramas llamadas: ej1, ej2, ..., ej7, ej8.
(hint: verificar listado de ramas y últimos commits con `git ls-remote`)

Se espera que se redacte una sección del README en donde se indique cómo ejecutar cada ejercicio y se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado (Parte 2) y los mecanismos de sincronización utilizados (Parte 3).

Se proveen [pruebas automáticas](https://github.com/7574-sistemas-distribuidos/tp0-tests) de caja negra. Se exige que la resolución de los ejercicios pase tales pruebas, o en su defecto que las discrepancias sean justificadas y discutidas con los docentes antes del día de la entrega. El incumplimiento de las pruebas es condición de desaprobación, pero su cumplimiento no es suficiente para la aprobación. Respetar las entradas de log planteadas en los ejercicios, pues son las que se chequean en cada uno de los tests.

La corrección personal tendrá en cuenta la calidad del código entregado y casos de error posibles, se manifiesten o no durante la ejecución del trabajo práctico. Se pide a los alumnos leer atentamente y **tener en cuenta** los criterios de corrección informados [en el campus](https://campusgrado.fi.uba.ar/mod/page/view.php?id=73393).

## Notas sobre la solución de los ejercicios

### Ejercicio N°1:

#### Interfaz y parámetros

El script se invoca en la raíz del proyecto con:

```bash
./generar-compose.sh <archivo_salida> <cantidad_clientes>
```

Ejemplo:

```bash
./generar-compose.sh docker-compose-dev.yaml 5
```

- **\$1**: nombre del archivo Compose a generar.
- **\$2**: cantidad de clientes a definir.

#### Flujo general (paso a paso)

1. **Reporte de parámetros**
   Imprime los valores recibidos para facilitar el diagnóstico:

   ```bash
   echo "Nombre del archivo de salida: $1"
   echo "Cantidad de clientes: $2"
   ```

2. **Creación/limpieza del archivo**
   Garantiza que el archivo de salida exista y, acto seguido, lo **sobrescribe** con el bloque YAML inicial (operador `>`):

   ```bash
   touch $1
   echo "name: tp0
   services:
     server:
       container_name: server
       image: server:latest
       entrypoint: python3 /main.py
       environment:
         - PYTHONUNBUFFERED=1
         - LOGGING_LEVEL=DEBUG
       networks:
         - testing_net" > $1
   ```

3. **Bucle de clientes**
   Genera **client1…clientN** con `seq` y **anexa** (operador `>>`) cada bloque al YAML:

   ```bash
   for i in $(seq 1 $2); do
       echo "
     client$i:
       container_name: client$i
       image: client:latest
       entrypoint: /client
       environment:
         - CLI_ID=$i
         - CLI_LOG_LEVEL=DEBUG
       networks:
         - testing_net
       depends_on:
         - server" >> $1
   done
   ```

   - Cada cliente:
     - Usa `container_name: client<i>` para cumplir el formato solicitado (client1, client2, …).
     - Pasa `CLI_ID=$i` y `CLI_LOG_LEVEL=DEBUG` como variables de entorno.
     - Declara dependencia de **server** con `depends_on` (orden de arranque).

4. **Definición de la red**
   Cierra el archivo con la red compartida por todos los servicios:

   ```bash
   echo "
   networks:
     testing_net:
       ipam:
         driver: default
         config:
           - subnet: 172.25.125.0/24" >> $1
   ```

   - `ipam.config.subnet` fija un rango predecible; útil para pruebas y debugging de conectividad.

#### Esquema resultante (ejemplo con 2 clientes)

```yaml
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net

  client1:
    container_name: client1
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=1
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server

  client2:
    container_name: client2
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=2
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
```

#### Evitación de short-writes en client

Se implementó la función `WriteFull`, que escribe un stream de bytes al socket utilizando un loop que chequea si todavía quedan bytes por escribir. Si efectivamente
faltan, se vuelve a escribir utilizando la función `Write`, que devuelve la cantidad de bytes escritos, pero no devuelve error si hubo un short-write.

```go
func writeFull(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
```

En `StartClientLoop` se utiliza esta función para escribir el mensaje.

```go
msg := fmt.Sprintf("[CLIENT %v] Message N°%v\n", c.config.ID, msgID)

if err := writeFull(c.conn, []byte(msg)); err != nil {
    log.Errorf("action: send | result: fail | client_id: %v | error: %v", c.config.ID, err)
    return
}
```

#### Evitación de short-reads y short-writes en server

El manejo de la conexión con el cliente en `__handle_client_connection` fue modificado, tal que la lectura y escritura ahora se ven así:

```python
try:
    rf = client_sock.makefile("rb")
    line = rf.readline(64 * 1024)
    if line == b"":
        raise EOFError("peer closed connection")
    msg = line.rstrip(b"\r\n").decode("utf-8")
    addr = client_sock.getpeername()
    logging.info(
        "action: receive_message | result: success | ip: %s | msg: %s",
        addr[0],
        msg,
    )
    client_sock.sendall((msg + "\n").encode("utf-8"))
except (UnicodeDecodeError, EOFError, OSError) as e:
    logging.error("action: receive_message | result: fail | error: %s", e)
finally:
    client_sock.close()
```

Se puede observar que se utiliza la función `makefile` para crear un stream de bytes para solo lectura conectado al socket, a partir del cual se leen hasta
64kB del socket con la función `readline`, que internamente hace los recv necesarios para evitar short-reads.
Por otro lado, para la escritura se utiliza `sendall`, que evita short-writes internamente, ya que garantiza enviar todo el buffer o fallar.

### Ejercicio N°2:

#### Cambios en el Dockerfile del cliente

- Se elimina la copia del `config.yaml` dentro de la imagen:
  - **Antes:** `COPY ./client/config.yaml /config.yaml`
  - **Ahora:** no se incluye el archivo de configuración en la imagen.

#### Cambios en `generar-compose.sh`

- **Servidor**: se monta el `config.ini` del host dentro del contenedor en modo **read-only**:

  ```yaml
  volumes:
    - ./server/config.ini:/config.ini:ro
  ```

- **Clientes**: se monta el `config.yaml` del host dentro de cada contenedor en modo **read-only**:

  ```yaml
  volumes:
    - ./client/config.yaml:/config.yaml:ro
  ```

#### Resultado

- La configuración queda **por fuera de la imagen** y se **inyecta** en tiempo de ejecución.
- Modificar `./server/config.ini` o `./client/config.yaml` en el host impacta inmediatamente en los contenedores al reiniciarlos, **sin reconstrucción** de imágenes.
- El montaje en `:ro` asegura que los contenedores **no** modifiquen los archivos de configuración del host.

### Ejercicio N°3:

#### Enfoque general

Se orquesta un **contenedor auxiliar** (`tester`) que contiene `netcat` y se conecta al servidor a través de una **red interna de Docker**.
De esta forma, la interacción se realiza **dentro del entorno de contenedores**, cumpliendo la restricción de no instalar herramientas en el host ni exponer puertos.

#### Flujo del script `validar-echo-server.sh`

1. **Lectura de configuración del servidor**
   Se extraen `SERVER_IP` y `SERVER_PORT` desde `./server/config.ini` con `awk`.

2. **Artefactos temporales con timestamp**
   Se generan nombres únicos para:
   - Archivo Compose temporal: `docker-compose-test-<timestamp>.yaml`
   - Directorio y Dockerfile de la imagen `tester`: `dockerfile-dir-test-<timestamp>/Dockerfile`

3. **Compose mínimo para el tester**
   Se crea un `docker-compose` con un único servicio `tester` conectado a la red `testing_net` (subred fija), suficiente para ejecutar la validación:

   ```yaml
   services:
     tester:
       image: tester:latest
       networks:
         - testing_net
   networks:
     testing_net:
       ipam:
         config:
           - subnet: 172.25.125.0/24
   ```

4. **Imagen `tester` basada en Alpine**
   Se define un Dockerfile temporal que:
   - Parte de `alpine:latest`.
   - Instala `netcat-openbsd`.
   - Mantiene el contenedor en ejecución con `CMD ["sleep", "infinity"]` para permitir `docker exec`.

5. **Despliegue y ejecución del test**
   - Se construye la imagen `tester:latest` y se levanta el servicio con `docker compose up -d`.
   - Se ejecuta, dentro del contenedor `tester`, el envío del mensaje con `netcat`:

     ```sh
     echo "test_msg_<timestamp>" | timeout 10 nc <SERVER_IP> <SERVER_PORT>
     ```

   - La **respuesta** se captura en la variable `respuesta`.

6. **Detención, baja y limpieza**
   Se detiene y elimina el stack temporal (`stop`/`down`) y se borran el Dockerfile/compose generados.

7. **Criterio de validación y salida requerida**
   - Si `respuesta` coincide exactamente con `test_msg_<timestamp>`, se imprime:

     ```
     action: test_echo_server | result: success
     ```

   - En caso contrario:

     ```
     action: test_echo_server | result: fail
     ```

#### Por qué no se exponen puertos ni se instala en el host

La comunicación se realiza **contenedor a contenedor** sobre la red `testing_net`. El contenedor `tester` actúa como cliente `netcat`, por lo que no se
requieren puertos publicados hacia el host ni instalación de herramientas fuera de Docker.

#### Elección de Alpine para la imagen temporal

Se utiliza **Alpine** por ser una base **ligera**, con un conjunto de herramientas **acotado** y suficiente para el objetivo del ejercicio.
Permite instalar `netcat-openbsd` de forma simple y mantener la imagen mínima necesaria para ejecutar la validación.

### Ejercicio N°4

#### Server

**Resumen.** El servidor atiende conexiones secuencialmente. Al recibir `SIGTERM`, deja de aceptar nuevas conexiones cerrando el **socket de escucha** y
sale del loop principal. Cada conexión se procesa de punta a punta (leer 1 línea, loguear, hacer echo, cerrar).

##### Puntos clave

- **Loop principal con corte por señal**

```python
def run(self):
    self._running = True
    signal.signal(signal.SIGTERM, self.__stop_running)
    while self._running:
        try:
            client_sock = self.__accept_new_connection()
            self.__handle_client_connection(client_sock)
        except OSError:
            if not self._running:
                break
            raise
    logging.shutdown()
```

- **Handler de SIGTERM**: marca el stop y **cierra el listener** para “desbloquear” `accept()` con un `OSError` controlado:

```python
def __stop_running(self, signum, frame):
    self._running = False
    self._server_socket.close()
```

**Qué garantiza el servidor**

- **No quedan bloqueos**: cerrar el listener hace que `accept()` despierte y el loop finalice.
- **Lecturas/escrituras completas**: `readline()` y `sendall()` manejan internamente **short read/write**.
- **Cierre ordenado**: cada socket del cliente se cierra en `finally`; al final, `logging.shutdown()` drena los buffers de log.

#### Cliente

**Resumen.** El cliente envía `LoopAmount` mensajes, uno cada `LoopPeriod`. Usa `signal.NotifyContext` para convertir `SIGTERM` en
**cancelación de contexto**, lo que le permite interrumpir de forma limpia: fija `ReadDeadline`, espera a terminar una lectura pendiente
y cierra la conexión actual.

##### Puntos clave

- **Contexto cancelable por SIGTERM**

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
defer stop()
```

- **Conexión por iteración + cierre garantizado**

```go
conn, derr := net.Dial("tcp", c.config.ServerAddress)
if derr != nil { /* log y return */ }
c.conn = conn
defer conn.Close() // cierra esta conexión pase lo que pase en la iteración
```

- **Lectura en goroutine + sincronización + timer**

```go
readDone := make(chan struct{})
var msg string
var err error
go func() {
    msg, err = bufio.NewReader(c.conn).ReadString('\n')
    close(readDone)
}()

timer := time.NewTimer(c.config.LoopPeriod)
select {
case <-ctx.Done():
    _ = c.conn.SetReadDeadline(time.Now()) // desbloquea ReadString
    <-readDone
    timer.Stop()
    c.conn.Close()
    return
case <-timer.C:
    select {
    case <-ctx.Done():
        _ = c.conn.SetReadDeadline(time.Now())
        <-readDone
        timer.Stop()
        c.conn.Close()
        return
    case <-readDone:
        c.conn.Close()
        if err != nil { /* log fail y return */ }
        log.Infof("action: receive_message | result: success | client_id: %v | msg: %v", c.config.ID, msg)
    }
}
```

**Qué garantiza el cliente**

- **Terminación limpia ante `SIGTERM`**: si el cliente está esperando respuesta, se fuerza un deadline de lectura para **desbloquear**
  la goroutine lectora, se espera a que termine y se cierran los recursos.
- **Sin short-write**: el `bufio.Writer` + `Flush()` aseguran que el mensaje se envíe completo antes de continuar.
- **Cierre explícito por iteración**: cada conexión se cierra antes de pasar al siguiente envío. El `defer conn.Close()` local
  asegura el cierre incluso ante retornos tempranos.

#### Resultado

Con este diseño, ambos procesos cumplen con **graceful shutdown**:

- El servidor **deja de aceptar nuevas conexiones** y **drena** la conexión en curso.
- El cliente **interrumpe lecturas bloqueadas**, **cierra la conexión** de la iteración y **termina ordenadamente**.
- Se evitan **short reads/writes** y se **registran** los eventos relevantes de cierre y de I/O.

### Ejercicio N°5:

#### Protocolo

El protocolo binario utiliza ordenamiento **little endian** y framing fijo:

```
| opcode: u8 | length: int32 | body (length bytes) |
```

Para describir el body se usa la notación:

- **\[int]**: entero de 4 bytes (int32 LE).
- **\[string]**: un \[int] `n` seguido de `n` bytes UTF-8.
- **\[string map]**: un \[int] `m` seguido de `m` pares `<k><v>` donde cada `<k>` y `<v>` es un \[string].
- **\[multi string map]**: un \[int] `n` seguido de `n` \[string map].

Mensajes implementados:

- **NEW_BETS (0)** — cliente → servidor. Body: un **\[multi string map]** que, en este ejercicio, contiene **1** \[string map] con las
  claves obligatorias: `AGENCIA`, `NOMBRE`, `APELLIDO`, `DOCUMENTO`, `NACIMIENTO`, `NUMERO`. Se elige este formato (en vez de un único map)
  para mantener compatibilidad futura con batches.
- **BETS_RECV_SUCCESS (1)** — servidor → cliente. Body: vacío.
- **BETS_RECV_FAIL (2)** — servidor → cliente. Body: vacío.

`length` indica la longitud exacta del body.

#### Client

El cliente está dividido en dos archivos dentro del paquete `app`:

- `protocol.go`: define el **formato de los mensajes** y su **serialización/deserialización** (transporte).
- `client.go`: maneja **conexiones**, **envío**, **espera de respuesta**, **graceful shutdown** y **logging** (aplicación).

**Flujo principal**

1. Se abre una conexión TCP al servidor.
2. Se construye un `NewBets` con un único **\[string map]** usando los campos provistos (incluyendo `AGENCIA` = `ClientConfig.ID`).
3. Se escribe el paquete completo respetando el framing; la escritura se hace con `io.Copy`/`bytes.Buffer.WriteTo`, que internamente reintenta hasta enviar
   todo el buffer, evitando **short writes**.
4. En paralelo, se queda a la espera de una única respuesta (`BETS_RECV_SUCCESS`/`BETS_RECV_FAIL`). La lectura usa `bufio.Reader` y `binary.Read` de
   tamaños fijos para evitar **short reads**.
5. Si llega `SIGTERM`, se convierte en cancelación de contexto; se fija un `ReadDeadline(time.Now())` para **desbloquear** la goroutine de lectura y
   cerrar la conexión de forma ordenada.
6. Según la respuesta, se imprime exactamente:
   - `action: apuesta_enviada | result: success | dni: ${DNI} | numero: ${NUMERO}`, o
   - el mismo log con `result: fail` ante error de protocolo/IO o respuesta de fallo.

**Detalle de transporte**

- `NewBets.WriteTo` arma en memoria `opcode | length | body` y lo vuelca a la conexión, minimizando syscalls y asegurando atomicidad lógica del paquete.
- `ReadMessage` consume `opcode` y delega en el lector específico del mensaje; las respuestas de éxito/fallo exigen `length == 0` para detectar corrimientos o basura.

#### Server

El servidor se organiza en tres módulos:

- `app/protocol.py`: **transporte** (opcodes, framing, lectura robusta). Implementa `recv_exactly` para leer **exactamente N bytes**, evitando **short reads**,
  y usa `sendall` en las escrituras, evitando **short writes**.
- `app/service.py`: **adaptación de dominio**. Convierte `RawBet` (transporte) a `utils.Bet` y delega en `utils.store_bets`.
- `app/net.py`: **aplicación**. Acepta conexiones, procesa un único `NEW_BETS` por conexión, responde con `BETS_RECV_SUCCESS`/`BETS_RECV_FAIL` y cierra el socket.
  Implementa **graceful shutdown** con `SIGTERM`: cierra el socket de escucha para despertar `accept()` y drenar el loop.

**Camino feliz (resumen)**

- `Server.run()` acepta una conexión.
- `recv_msg()` parsea `NEW_BETS` validando estructura y longitudes.
- Se responde **SUCCESS** y luego se persiste con `service.store_bets(...)`.
- Por cada apuesta persistida se emite (según implementación del servicio) el log:

  ```
  action: apuesta_almacenada | result: success | dni: ${DNI} | numero: ${NUMERO}
  ```

**Robustez de E/S**

- **Lectura**: `recv_exactly` reintenta hasta completar; `read_i32`/`read_u8`/`read_string` controlan tamaños y UTF-8. Se lleva un contador `remaining` para
  asegurar que los bytes consumidos coincidan con `length`; discrepancias levantan `ProtocolError`.
- **Escritura**: las respuestas usan `sock.sendall`, que bloquea hasta enviar todo el buffer o falla, evitando **short writes**.

### Ejercicio N°6

**Estructura:** se mantiene la separación de responsabilidades de E5: el **cliente** se encarga de conexión + batching; el **servidor** de aceptar sockets,
parsear frames y persistir; el **módulo de protocolo** define el <!-- wire  -->format y las primitivas de (de)serialización.

---

#### Cliente

**Flujo general**

1. Lee el CSV de la agencia y **acumula apuestas** en memoria.
2. Antes de agregar cada apuesta al cuerpo del batch, verifica dos límites:
   - **Tamaño máximo de paquete:** no exceder **8 KiB** incluyendo `opcode(1) + length(4) + n(4) + body`.
   - **Cantidad máxima por batch:** `BatchLimit` (desde `config.yaml`).

3. Si al sumar la apuesta actual se excede alguno de los límites, **emite el batch** (flush), vacía el buffer y comienza un batch nuevo con la apuesta actual.
4. Al terminar el archivo (o si el contexto se cancela), hace **flush final** si queda contenido pendiente.
5. Cierra el lado de escritura del TCP (`CloseWrite`) y **lee todas las respuestas** del servidor hasta EOF. Loguea _success_ sólo si la última
   respuesta fue `BETS_RECV_SUCCESS` y no hubo errores de lectura.

**Piezas clave**

- **Acumulación + decisión de flush**

```go
// Serializa la apuesta y decide si entra en el batch actual.
// Si no entra (por tamaño o cantidad), flushea y comienza un batch nuevo.
func AddBetWithFlush(bet map[string]string, to *bytes.Buffer, out io.Writer,
    betsCounter *int32, batchLimit int32) error {

    var encoded bytes.Buffer
    if err := writeStringMap(&encoded, bet); err != nil {
        return err
    }
    // 1 (opcode) + 4 (length) + 4 (nBets)
    const headerOverhead = 1 + 4 + 4
    fitsSize := to.Len()+encoded.Len()+headerOverhead <= 8*1024
    fitsCount := *betsCounter+1 <= batchLimit

    if fitsSize && fitsCount {
        _, _ = io.Copy(to, &encoded)
        *betsCounter++
        return nil
    }
    if err := FlushBatch(to, out, *betsCounter); err != nil {
        return err
    }
    if err := writeStringMap(to, bet); err != nil {
        return err
    }
    *betsCounter = 1
    return nil
}
```

- **Emisión del batch (framing exacto)**

```go
func FlushBatch(body *bytes.Buffer, out io.Writer, nBets int32) error {
    // opcode
    if err := binary.Write(out, binary.LittleEndian, NewBetsOpCode); err != nil { return err }
    // length = 4 (nBets) + body.Len()
    if err := binary.Write(out, binary.LittleEndian, int32(4+body.Len())); err != nil { return err }
    // nBets
    if err := binary.Write(out, binary.LittleEndian, nBets); err != nil { return err }
    // body
    if _, err := io.Copy(out, body); err != nil { return err }
    body.Reset()
    return nil
}
```

> **Short write:** evitado porque todas las escrituras del cuerpo se hacen con `io.Copy`/`bytes.Buffer.WriteTo`, que reintentan hasta completar o fallar.

- **Recepción de respuestas y _graceful shutdown_**

```go
// Tras enviar, cierro escritura para que el server detecte EOF y me responda todo.
if tcp, ok := c.conn.(*net.TCPConn); ok {
    _ = tcp.CloseWrite()
}

reader := bufio.NewReader(c.conn)
readDone := make(chan struct{})
var last Readable
var rerr error

go func() {
    for {
        last, rerr = ReadMessage(reader)
        if rerr != nil {
            break
        }
    }
    close(readDone)
}()

select {
case <-ctx.Done():
    // Desbloquea la lectura y termina ordenado
    _ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
    <-readDone
    return
case <-readDone:
    if (rerr != nil && !errors.Is(rerr, io.EOF)) ||
       (last != nil && last.GetOpCode() == BetsRecvFailOpCode) {
        log.Error("action: bets_enviadas | result: fail")
        return
    }
    log.Info("action: bets_enviadas | result: success")
}
```

> **Short read:** oculto a la lógica de aplicación gracias al framing y al `bufio.Reader`; si TCP entrega menos bytes, el parser sigue leyendo hasta completar
> el frame o falla.

---

#### Servidor

**Flujo general**

- Acepta conexiones de forma **secuencial** (como en E5).
- Por cada socket, **lee un mensaje** `NEW_BETS`, valida y decodifica el batch, intenta **persistir todas las apuestas** y responde:
  - `BETS_RECV_SUCCESS` si **todas** se procesaron ok.
  - `BETS_RECV_FAIL` si ocurre **cualquier** error de framing/validación/persistencia.

- Loguea por cada apuesta persistida y un resumen por batch (success/fail con la cantidad).

**Piezas clave**

- **Lectura exacta del cuerpo** (_short read_‐safe):

```python
def recv_exactly(sock: socket.socket, n: int) -> bytes:
    """Reads exactly n bytes or raises on premature EOF."""
    if n < 0:
        raise ProtocolError("invalid body")
    buf = bytearray(n)
    view = memoryview(buf)
    read = 0
    while read < n:
        nrecv = sock.recv_into(view[read:], n - read)
        if nrecv == 0:
            raise EOFError("peer closed connection")
        read += nrecv
    return bytes(buf)
```

- **Framing + validación “todo-o-nada”** (mapas con claves requeridas):

```python
def recv_msg(sock: socket.socket):
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid length")
    if opcode == Opcodes.NEW_BETS:
        msg = NewBets()
        msg.read_from(sock, length)   # consume exactamente 'length' bytes
        return msg
    raise ProtocolError(f"invalid opcode: {opcode}")
```

La clase `NewBets` exige exactamente **6 pares `<k,v>`** por apuesta y la presencia de las claves: `AGENCIA, NOMBRE, APELLIDO, DOCUMENTO, NACIMIENTO, NUMERO`.
Ante cualquier inconsistencia, **descarta el batch completo** (responde _fail_).

- **Respuestas sin _short write_**:

```python
def write_u8(sock, value: int) -> None:
    if not 0 <= value <= 255:
        raise ValueError("u8 out of range")
    sock.sendall(bytes([value])) # escritura completa o excepción


def write_i32(sock: socket.socket, value: int) -> None:
    sock.sendall(int(value).to_bytes(4, byteorder="little", signed=True))# escritura completa o excepción
```

- **Logging y contrato de batch**:

```python
try:
    service.store_bets(msg.bets)
    for b in msg.bets:
        logging.info(
            "action: apuesta_almacenada | result: success | dni: %s | numero: %s",
            b.document, b.number,
        )
    protocol.BetsRecvSuccess().write_to(client_sock)
    logging.info(
        "action: apuesta_recibida | result: success | cantidad: %i", msg.amount
    )
except Exception as e:
    protocol.BetsRecvFail().write_to(client_sock)
    logging.error(
        "action: apuesta_recibida | result: fail | cantidad: %i", getattr(msg, "amount", 0)
    )
```

- **Terminación ordenada (SIGTERM)**: igual que en E5. Un handler cierra el socket de escucha para **desbloquear `accept()`**; el loop verifica el flag y sale,
  ejecutando por último `logging.shutdown()`.

---

#### Resumen

- **Batching con límites**: tamaño total ≤ 8 KiB y `BatchLimit` configurable.
- **Framing binario LE**: `opcode(1) | length(i32) | n(i32) | body`.
- **Todo-o-nada**: el servidor sólo responde _success_ si **todas** las apuestas del batch se persistieron correctamente.
- **Robustez I/O**: `io.Copy`/`sendall` evitan _short write_; `recv_exactly` y parsers con `remaining` evitan _short read_ y desalineaciones.
- **Graceful shutdown** en cliente (contexto + deadlines) y servidor (SIGTERM + cierre del listener).

Con esta versión, el sistema procesa archivos de apuestas por **lotes eficientes**, minimizando overhead de red, manteniendo la **integridad del protocolo**
y cumpliendo exactamente con los logs que pide el enunciado.

### Ejercicio N°7:

#### Actualización del protocolo

Se agrega una nueva notación para describir el formato del body: \[string list], que consta de un n \[int] seguido de n \[string].

Se agregan dos mensajes nuevos:

- `FINISHED`, con opcode **3**, que utilizará el cliente para avisar al servidor que terminó con la entrega de todos los batches, solicitando
  los ganadores al mismo tiempo. El body es un \[int] que contendrá el ID de la agencia.
- `WINNERS`, con opcode **4**, que utilizará el server para notificar los ganadores correspondientes a cada agencia. El body es un \[string list],
  que contendrá los DNI de todos los ganadores.

---

#### Client

**1) Aviso de finalización del envío (FINISH)**
Al concluir el envío de todos los batches, el cliente notifica al servidor en la **misma conexión** con el `agencyId` en el body:

```go
agencyId, _ := strconv.Atoi(c.config.ID)
finishedMsg := Finished{int32(agencyId)}
_, _ = finishedMsg.WriteTo(c.conn)
log.Infof("action: send_finished | result: success | agencyId: %d", int32(agencyId))
```

Si el servidor todavía no realizó el sorteo, encola el socket del cliente junto con el id de la agencia, de manera que el cliente queda esperando a
los ganadores.

**3) Lectura estricta de `WINNERS` (evitando short reads)**
El cliente valida la longitud del body y cada string con un contador `remaining`, y usa `io.ReadFull` para garantizar lecturas completas:

```go
func (msg *Winners) readFrom(reader *bufio.Reader) error {
    var remaining int32
    _ = binary.Read(reader, binary.LittleEndian, &remaining)

    var nWinners int32
    _ = binary.Read(reader, binary.LittleEndian, &nWinners)
    remaining -= 4

    for i := int32(0); i < nWinners; i++ {
        var strLen int32
        _ = binary.Read(reader, binary.LittleEndian, &strLen)
        remaining -= 4

        buf := make([]byte, int(strLen))
        _, _ = io.ReadFull(reader, buf)
        remaining -= strLen

        msg.List = append(msg.List, string(buf))
    }
    if remaining != 0 { return &ProtocolError{"invalid body length", msg.GetOpCode()} }
    return nil
}
```

**4) E/S confiable e interrupción ordenada**

- **Escrituras**: `io.Copy` en los envíos de body y `binary.Write` para campos fijos sobre el `net.Conn` (que internamente pueden fragmentarse) junto con cierres
  explícitos de conexión por intento mitigan _short writes_.
- **Lecturas**: `io.ReadFull` asegura consumir exactamente `strLen` bytes por string.

---

#### Server

**1) Estado y flujo secuencial**
El servidor mantiene `_finished` (una cola con las agencias que enviaron `FINISH` y sus sockets), `_raffle_done` (sorteo realizado)
y `_winners` (DNIs ganadores por agencia).
La atención es **secuencial**: cada socket procesa mensajes del cliente hasta que éste envía `FINISHED`. Si todavía faltan apuestas de otras agencias,
se encola esa conexión y se pasa a aceptar otro cliente. Una vez que todos enviaron `FINISHED`, se comienzan a desencolar los clientes y se le envían
los ganadores correspondientes a su agencia, cerrando la conexión en el momento.

```python
def __process_msg(self, msg, client_sock: socket.socket) -> HandleConnAction:
    if msg.opcode == protocol.Opcodes.NEW_BETS:
        try:
            service.store_bets(msg.bets)
            for bet in msg.bets:
                logging.info(
                    "action: apuesta_almacenada | result: success | dni: %s | numero: %s",
                    bet.document,
                    bet.number,
                )
        except Exception as e:
            protocol.BetsRecvFail().write_to(client_sock)
            logging.error(
                "action: apuesta_recibida | result: fail | cantidad: %d", msg.amount
            )
            return HandleConnAction.CLOSE # indica que la conexión debe cerrarse
        logging.info(
            "action: apuesta_recibida | result: success | cantidad: %d",
            msg.amount,
        )
        protocol.BetsRecvSuccess().write_to(client_sock)
        return HandleConnAction.KEEP # indica que la conexión no debe cerrarse

    if msg.opcode == protocol.Opcodes.FINISHED:
        self._finished.append((msg.agency_id, client_sock)) # encolar conexion
        if len(self._finished) == self._clients_amount:
            self.__raffle() # terminó el último cliente -> sorteo
            while self._finished:
                (ag_id, sock) = self._finished.popleft()
                self.__send_winners(ag_id, sock) # desencolo cliente y envío `WINNERS` con DNIs de su agencia
                sock.close()
        return HandleConnAction.EXIT # indica que no debe continuarse la comunicación con el cliente, pero no debe cerrarse la conexión
```

**2) Recepción de apuestas y ACK**
Al recibir `NEW_BETS`, se validan y almacenan las apuestas. Luego se responde `BETS_RECV_SUCCESS` y se sigue leyendo en la misma conexión (hasta el `FINISHED`).
La deserialización usa un `recv_exactly` que evita _short reads_:

```python
def recv_exactly(sock: socket.socket, n: int) -> bytes:
    data = bytearray(n); view = memoryview(data); read = 0
    while read < n:
        nrecv = sock.recv_into(view[read:], n - read)
        if nrecv == 0: raise EOFError("peer closed connection")
        read += nrecv
    return bytes(data)
```

**3) Coordinación de sorteo**
El sorteo se dispara **una sola vez** cuando se reciben los `FINISHED` de todas las agencias y se agrupan ganadores por agencia:

```python
def __raffle(self):
    """Compute winners once all agencies finished.

    Delegates to service.compute_winners() and stores the result.
    Logs success/failure. Intended to run exactly once.
    """
    try:
        self._winners = service.compute_winners()
        self._raffle_done = True
        logging.info("action: sorteo | result: success")
    except Exception as e:
        logging.error("action: sorteo | result: fail | error: %s", e)
        return
```

**4) Envío de `WINNERS` y cierre temprano**

- Si el sorteo está listo y la agencia figura en `_finished`, se envía `WINNERS` y se cierra la conexión.

La construcción del mensaje `WINNERS` usa `sendall` para evitar _short writes_:

```python
class Winners:
    def write_to(self, sock):
        body_length = 4
        for document in self.list:
            body_length += 4 + len(document)
        write_u8(sock, Opcodes.WINNERS)
        write_i32(sock, body_length)
        write_i32(sock, len(self.list))
        for document in self.list:
            write_string(sock, document)  # sendall
```

**5) Cantidad de agencias**
La cantidad de agencias es configurable en el script `generar-compose.sh`, ya que se pasa el segundo parámetro del mismo como variable de entorno al servicio del server,
que utiliza esta variable para saber cuántos mensajes `Finished` debe esperar para hacer el sorteo.

**6) Graceful Shutdown**

En el handler de `SIGTERM` se desencolan los sockets que habían sido encolados y se cierran esas conexiones, para liberar los recursos correctamente.

```python
def __stop_running(self, _signum, _frame):
    """SIGTERM handler.

    Marks the server as stopping and closes the listening socket
    to wake up accept().
    """
    self._running = False
    self._server_socket.close()
    while self._finished:
        _ag, sock = self._finished.popleft()
        try:
            sock.close()
        except OSError:
            pass
```

### Ejercicio N°8

#### Objetivo y cambio principal

En este ejercicio se mantiene el **batching** de apuestas y se agrega **concurrencia real en el servidor**. El cliente deja de hacer _polling_: tras enviar todos sus
batchs, **bloquea** esperando el mensaje `WINNERS`. El servidor pasa a ser **multithreaded** (un hilo por conexión), coordinando un **sorteo único** y respondiendo a
cada agencia con su lista de ganadores.

---

#### Protocolo (resumen)

Framing binario `little-endian`:

```
opcode (u8) | length (i32) | body (length bytes)
```

Mensajes:

- `NEW_BETS` (0) – cliente→server. Body: `[int n_bets]` + n_bets × **\[string map]** de 6 pares (`AGENCIA`, `NOMBRE`, `APELLIDO`, `DOCUMENTO`, `NACIMIENTO`, `NUMERO`).
- `BETS_RECV_SUCCESS` (1) / `BETS_RECV_FAIL` (2) – server→cliente. Body vacío.
- `FINISHED` (3) – cliente→server. Body: `[int agency_id]`.
- `WINNERS` (4) – server→cliente. Body: `[int count]` + `count × [string]` (DNIs ganadores).

El servidor **valida longitudes** y **consume exactamente** los bytes del body (evita _short read_). Las escrituras usan `sendall`/`io.Copy` (evita _short write_).

---

#### Cliente (Go): batching + espera bloqueante de ganadores

1. **Batching seguro**
   - Se leen apuestas del CSV y se agregan al buffer con `AddBetWithFlush`.
   - Se verifica **límite de 8 KiB** (incluyendo `opcode + length + n`) y **BatchLimit**; si no entra, se hace **flush** del batch actual (`FlushBatch`) y se
     inicia uno nuevo.
   - En EOF o cancelación, se **flushea** el batch parcial.

2. **Ciclo de envío/recepción**
   - Se abre una única conexión TCP.
   - Goroutine `readResponse` consume todas las respuestas (`BETS_RECV_*`) y se **detiene al recibir `WINNERS`** (no hay polling).
   - Al terminar de enviar, el cliente manda `FINISHED` y **bloquea** hasta `WINNERS` o cancelación
     (`SIGTERM` → `SetReadDeadline` para desbloquear la lectura y cerrar limpio).

3. **Robustez**
   - Escrituras de batches: `binary.Write` + `io.Copy` (evita _short write_).
   - Lector con `bufio.Reader` y decodificación por framing (evita _short reads_).
   - Logs requeridos: `bets_enviadas`, `consulta_ganadores`, errores.

---

#### Servidor (Python): multithreading con coordinación

El servidor atiende **múltiples conexiones en paralelo** (un hilo por cliente) y coordina tres hitos:

- **Persistencia de apuestas** (`NEW_BETS`)
  Se guarda el batch completo bajo `_storage_lock` (serializa E/S a disco).
  - Éxito → `BETS_RECV_SUCCESS` y `action: apuesta_recibida | success | cantidad`.
  - Falla → `BETS_RECV_FAIL` y `action: apuesta_recibida | fail | cantidad`.

- **Sincronización de “todos terminaron”** (`FINISHED`)
  Cada hilo espera en una `threading.Barrier` de **clients_amount** size (pasado por parámetro al script `generar-compose.sh`).
  Cuando llega el último, un hilo toma el `'_raffle_lock'` y dispara el **sorteo único** y setea `_raffle_done`.
  Una vez que el sorteo está hecho, los hilos envían `WINNERS` con los DNIs de la agencia que envió el `FINISHED`.

**Primitivas y por qué:**

- `threading.Barrier(5)` → asegura que el sorteo ocurra **solo** cuando **todas** las agencias enviaron `FINISHED`.
- `threading.Lock` (`_raffle_lock`, `_storage_lock`) → protege **sorteo único** y **persistencia** contra carreras.
- Un hilo por conexión → simplicidad; el servidor escala bien para el **I/O-bound** del problema con la poca cantidad de clientes que se manejan.

Manejo de cierre: `SIGTERM` → se setea `_stop`, se cierra el listener para despertar `accept()`, se **joinean** los workers y se hace `logging.shutdown()`.

```python
def __process_msg(self, msg, client_sock) -> bool:
    if msg.opcode == protocol.Opcodes.NEW_BETS:
        try:
            with self._storage_lock: # store_bets no es thread-safe
                service.store_bets(msg.bets)
                for bet in msg.bets:
                    logging.info(
                        "action: apuesta_almacenada | result: success | dni: %s | numero: %s",
                        bet.document,
                        bet.number,
                    )
        except Exception as e:
            protocol.BetsRecvFail().write_to(client_sock)
            logging.error(
                "action: apuesta_recibida | result: fail | cantidad: %d", msg.amount
            )
            return True
        logging.info(
            "action: apuesta_recibida | result: success | cantidad: %d",
            msg.amount,
        )
        protocol.BetsRecvSuccess().write_to(client_sock)
        return True
    if msg.opcode == protocol.Opcodes.FINISHED:
        self._finished.wait() # esperar a que todos los clientes terminen
        with self._raffle_lock: # load_bets (utilizado en __raffle) no es thread_safe
                                # además asegura que el sorteo se realice una sola vez
            if not self._raffle_done.is_set():
                self.__raffle()
        self.__send_winners(msg.agency_id, client_sock)
        return False
```

---

#### Justificación de utilización de multithreading en vez de multiprocessing

**Perfil I/O-bound + estado global coherente**
Aunque CPython tenga GIL, **las llamadas de red y archivo liberan el GIL**. Nuestro workload es mayormente **E/S** (aceptar conexiones, `recv`, `sendall`,
leer/escribir CSV). Con hilos obtenemos **concurrencia real** en I/O y mantenemos **un único espacio de memoria** para:

- **Apuestas persistidas**
- **Barrera** de “todas las agencias terminaron”
- **Sorteo único** (`_raffle_lock` + `_raffle_done`)
- **Ganadores por agencia** (`_winners`)

Con **multiprocessing** el estado quedaría **particionado** entre procesos. Sin un router “sticky” por agencia o un **coordinador externo** (DB/IPC),
**las apuestas de una misma agencia pueden dispersarse** si el cliente quisiera desconectarse entre el envío de batches, complicando:

- **Sorteo único** (habría que centralizarlo/coordinarlo explícitamente).
- **Entrega de ganadores correctos** por agencia.
- **Persistencia concurrente interproceso** (precisa file locking a nivel SO).
- **Barreras** interproceso (no hay un reemplazo tan directo como `threading.Barrier`).

**Menos complejidad operativa**: con hilos evitamos colas/Managers/locks interproceso y mantenemos la sincronización en memoria (Locks/Barrier/Event),
suficiente y clara para **5 agencias**. **Overhead** también menor (crear threads es más liviano que procesos).

> Conclusión: para este caso (pocas agencias, E/S predominante, **sorteo único** y **estado compartido**), se considera que **multithreading** es la
> opción más simple y mantenible.

---

#### Flujo end-to-end

1. Cada cliente lee su CSV (`agency-{N}.csv`), arma **batches ≤ 8 KiB** y los envía.
2. Por cada `NEW_BETS`, el server responde **success/fail** y persiste bajo lock.
3. Cada cliente envía `FINISHED`.
4. El server espera que todas las agencias crucen la **Barrier**, corre el **sorteo único** una sola vez, **setea** `_raffle_done`.
5. El server responde `WINNERS` con DNIs de esa agencia; el cliente **desbloquea** y termina.

---

#### Manejo de errores y robustez

- **Short reads/writes**:
  - Server: `recv_exactly` (lee **exactamente n** bytes) + `sendall`.
  - Cliente: `io.Copy` para el body + `binary.Write` para encabezados.

- **Draining en parse fallido**: si se detecta un error de framing, se **drenan** los bytes restantes del body para mantener el stream sincronizado.
- **Graceful shutdown**: `SIGTERM` corta `accept()`, se espera a los workers, se cierran sockets y se drenan logs.

Con este diseño, el sistema logra: **concurrencia** de conexiones, **estado global consistente**, **sorteo único** garantizado, y
**contrato de protocolo** estricto (framing, validaciones y logs requeridos).

## Apéndice

### Falta de política de reintentos / timeouts de conexión

Para mantener una solución relativamente simple y asegurar una implementación correcta en poco tiempo, se decidió priorizar el _happy path_ y presuponer
un correcto comportamiento del cliente, tal como fue implementado. Debido a eso, no se agregaron timeouts del servidor en caso de que el
cliente tardara demasiado entre batch y batch de bets, y en consecuencia tampoco se esgrimió una política de reintentos por parte del cliente en caso de
desconexión/error del servidor.
Aunque esto no es lo ideal, se consideró que no supone un problema demasiado crítico para el correcto funcionamiento del servidor y de la solución en general.
Además, dado que se utilizan threads, no es tan costoso en términos de memoria tener algunos threads ociosos en espera de respuestas, como sí lo sería en un
esquema multiprocessing. En cuanto al servidor secuencial del ejercicio 7, se mantendrían sockets abiertos durante mucho tiempo, algo que, aunque inconveniente,
tampoco invalida la solución.
Por último, dado que el enunciado estipula que los ganadores deben enviarse una vez que las apuestas de todos los clientes fueron recibidas y el sorteo realizado,
se implementen timeouts o no, los clientes deberán esperar la misma cantidad de tiempo en caso de que otro cliente tarde un tiempo excesivo en enviar sus bets.

### Errores encontrados y posible solución

No se agrega un finally al método `run` del `Server` en caso de OSError no asociado a un `SIGTERM`, haciendo que en ese caso los threads no se joineen.
Para resolverlo, habría que actualizar la función `run`, encapsulando al join de los threads en un bloque `finally`:

```python
def run(self):
    """Main server loop.

    Installs SIGTERM handler, accepts connections until `_stop` is set,
    and spawns one worker thread per client. On shutdown:
    - breaks the accept loop if the listening socket is closed,
    - joins all worker threads,
    - and calls `logging.shutdown()` to flush logs.
    """
    signal.signal(signal.SIGTERM, self.__handle_sigterm)
    try:
        while not self._stop.is_set():
            try:
                client_sock = self.__accept_new_connection()
                t = threading.Thread(
                    target=self.__handle_client_connection, args=(client_sock,)
                )
                self._threads.append(t)
                t.start()
            except OSError:
                if self._stop.is_set():
                    break
                raise
    finally: # debería agregarse un finally para que se ejecute siempre
        for t in self._threads:
            t.join()
        logging.shutdown()
```

Por otro lado, en el `handle_sigterm` no se cierran los sockets de los threads, generando la posibilidad de que un thread quede bloqueado indefinidamente
o durante un tiempo excesivamente largo en espera de un mensaje de un cliente que dejó de comunicarse. Para mitigar este riesgo, lo mejor sería guardar
los threads con sus respectivos sockets, e iterar esa lista en el `handle_sigterm`, forzando el cierre de los sockets de cada uno.
