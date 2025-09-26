import logging
import signal
import socket
import threading

from app import protocol


class Orchestrator:
    def __init__(self, port, listen_backlog):
        """Initialize listening socket and concurrency primitives.

        - Creates and binds the TCP listening socket.
        - `_stop` is a process-wide shutdown flag (set by SIGTERM).
        - `_finished` is a Barrier with the expected number of clients/agencies;
          it is used to block FINISHED handlers until all are in.
        - `_threads` keeps track of per-connection worker threads.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        # self._filter_router_queue = new MessageMiddlewareQueue('host','queue_name')

    def run(self):
        """Main server loop.

        Installs SIGTERM handler, accepts connections until `_stop` is set,
        and spawns one worker thread per client. On shutdown:
        - breaks the accept loop if the listening socket is closed,
        - joins all worker threads,
        - and calls `logging.shutdown()` to flush logs.
        """
        signal.signal(signal.SIGTERM, self.__handle_sigterm)
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
        for t in self._threads:
            t.join()
        logging.shutdown()

    def __accept_new_connection(self):
        """Accept a single client connection.

        Blocks in `accept()` until a client connects, logs the remote IP,
        and returns the connected socket.
        """
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c

    def __handle_client_connection(self, client_sock):
        """Per-connection worker.

        Repeatedly receives framed messages (`protocol.recv_msg`), logs them,
        and delegates handling to `__process_msg`. The loop continues until
        `__process_msg` returns False (connection should close), `_stop` is set,
        EOF is reached, or a socket/protocol error occurs. Always closes the
        client socket on exit.
        """
        while not self._stop.is_set():
            msg = None
            try:
                msg = protocol.recv_msg(client_sock)
                addr = client_sock.getpeername()
                logging.info(
                    "action: receive_message | result: success | ip: %s | opcode: %i",
                    addr[0],
                    msg.opcode,
                )
                if not self.__process_msg(msg, client_sock):
                    break
            except protocol.ProtocolError as e:
                logging.error("action: receive_message | result: protocol error | error: %s", e)
                break  # Close connection on protocol errors to prevent stream corruption
            except EOFError:
                break
            except OSError as e:
                logging.error("action: send_message | result: fail | error: %s", e)
                break
        client_sock.close()

    def __process_msg(self, msg, client_sock) -> bool:
        """Process a decoded message.""" # <-- CORRECCIÓN 2: Docstring simplificado
        if msg.opcode != protocol.Opcodes.FINISHED and msg.opcode != protocol.Opcodes.BETS_RECV_SUCCESS and msg.opcode != protocol.Opcodes.BETS_RECV_FAIL:
            try:
                # Mapear el status a texto legible
                status_names = {0: "Continue", 1: "EOF", 2: "Cancel"}
                status_text = status_names.get(msg.batch_status, f"Unknown({msg.batch_status})")
                
                # 1. Escribir el mensaje original recibido
                with open("received_messages.txt", "a", encoding="utf-8") as f:
                    f.write(f"=== Mensaje ORIGINAL - Opcode: {msg.opcode} - Cantidad: {msg.amount} - Batch: {msg.batch_number} - Status: {status_text} ===\n")
                    for i, row in enumerate(msg.rows):
                        f.write(f"Row {i+1}: {row.__dict__}\n")
                    f.write("\n")
                
                # 2. Crear mensaje filtrado usando la nueva función
                try:
                    filtered_batch = protocol.create_filtered_data_batch(msg)
                    
                    # Generar los bytes y loggear información
                    batch_bytes = filtered_batch.to_bytes()
                   
                    # self._filter_router_queue.send(batch_bytes)
                    
                    # 3. Escribir el mensaje filtrado a un archivo separado
                    with open("filtered_messages.txt", "a", encoding="utf-8") as f:
                        f.write(f"=== Mensaje FILTRADO - Tabla: {filtered_batch.filtered_data['table_name']} - Original: {filtered_batch.filtered_data['original_row_count']} - Filtrado: {filtered_batch.filtered_data['filtered_row_count']} - Batch: {filtered_batch.filtered_data['batch_number']} - Status: {status_text} ===\n")
                        f.write(f"Query IDs: {filtered_batch.query_ids}\n")
                        f.write(f"Table IDs: {filtered_batch.table_ids}\n")
                        f.write(f"Total Shards: {filtered_batch.total_shards}, Shard Num: {filtered_batch.shard_num}\n")
                        for i, row in enumerate(filtered_batch.filtered_data['rows']):
                            f.write(f"Row {i+1}: {row}\n")
                        f.write("\n")
                        
                    # Log del filtrado
                    logging.info(
                        "action: batch_filtered | table: %s | original_count: %d | filtered_count: %d | batch_number: %d | status: %s",
                        filtered_batch.filtered_data['table_name'],
                        filtered_batch.filtered_data['original_row_count'],
                        filtered_batch.filtered_data['filtered_row_count'],
                        filtered_batch.filtered_data['batch_number'],
                        status_text
                    )
                    
                except Exception as filter_error:
                    logging.warning(
                        "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
                        getattr(msg, 'batch_number', 0), msg.opcode, str(filter_error)
                    )
                
            except Exception as e:
                protocol.BetsRecvFail().write_to(client_sock)
                logging.error(
                    "action: batch_recibido | result: fail | batch_number: %d | cantidad: %d | error: %s", 
                    getattr(msg, 'batch_number', 0), msg.amount, str(e)
                )
                return True
            
            # Log mejorado con batch_number y status
            logging.info(
                "action: batch_recibido | result: success | opcode: %d | cantidad: %d | batch_number: %d | status: %s",
                msg.opcode, msg.amount, msg.batch_number, status_text
            )
            
            # Log adicional con preview de datos
            try:
                if msg.rows and len(msg.rows) > 0:
                    sample_rows = msg.rows[:2]  # Primeras 2 filas como muestra
                    all_keys = set()
                    for row in sample_rows:
                        all_keys.update(row.__dict__.keys())
                    
                    sample_data = [row.__dict__ for row in sample_rows]
                    
                    logging.debug(
                        "action: batch_preview | batch_number: %d | status: %s | opcode: %d | keys: %s | sample_count: %d | sample: %s",
                        msg.batch_number, status_text, msg.opcode, sorted(list(all_keys)), len(sample_rows), sample_data
                    )
            except Exception:
                logging.debug("action: batch_preview | batch_number: %d | result: skip", getattr(msg, 'batch_number', 0))
                
            protocol.BetsRecvSuccess().write_to(client_sock)
            return True
            
        if msg.opcode == protocol.Opcodes.FINISHED:
            return False
        
    def __handle_sigterm(self, *_):
        """SIGTERM handler.

        Sets the global stop flag and closes the listening socket to unblock
        `accept()`. Worker threads already running will drain naturally and
        be joined in `run()`.
        """
        self._stop.set()
        self._server_socket.close()
