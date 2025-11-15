import os
import signal
import threading
import logging
from results_finisher import ResultsFinisher
from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Main] - %(message)s')

def main():
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    input_queue_name = os.getenv("INPUT_QUEUE", "results_finisher_queue_1")
    output_queue_name = os.getenv("OUTPUT_QUEUE", "orchestrator_results_queue")
    
    finisher_index_env = os.getenv("RESULTS_FINISHER_INDEX")
    if finisher_index_env is not None:
        finisher_index = int(finisher_index_env)
    else:
        finisher_index = hash(os.getenv("HOSTNAME", "results-finisher")) % 100
    election_port = int(os.getenv("ELECTION_PORT", 9600 + finisher_index))
        
    logging.info("--- ResultsFinisher Service (In-Memory) ---")
    logging.info(f"RabbitMQ Host: {rabbitmq_host}")
    logging.info(f"Input Queue: {input_queue_name}")
    logging.info(f"Output Queue: {output_queue_name}")
    logging.info("-------------------------------------------")

    try:
        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue_name)
        output_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue_name)

        heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2.0"))
        heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "1.0"))
        heartbeat_max_misses = int(os.getenv("HEARTBEAT_MAX_MISSES", "3"))
        heartbeat_startup_grace = float(os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS", "4.0"))
        heartbeat_election_cooldown = float(os.getenv("HEARTBEAT_ELECTION_COOLDOWN_SECONDS", "5.0"))
        heartbeat_cooldown_jitter = float(os.getenv("HEARTBEAT_COOLDOWN_JITTER_SECONDS", "1.0"))
        follower_down_timeout = float(os.getenv("FOLLOWER_DOWN_TIMEOUT_SECONDS", "15.0"))
        follower_restart_cooldown = float(os.getenv("FOLLOWER_RESTART_COOLDOWN_SECONDS", "30.0"))
        follower_recovery_grace = float(os.getenv("FOLLOWER_RECOVERY_GRACE_SECONDS", "10.0"))

        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None

        def handle_leader_change(new_leader_id: int, am_i_leader: bool):
            logging.info(
                "Leader update | new_leader=%s | am_i_leader=%s",
                new_leader_id,
                am_i_leader,
            )
            if heartbeat_client:
                if am_i_leader:
                    heartbeat_client.deactivate()
                else:
                    heartbeat_client.activate()
            if follower_recovery:
                follower_recovery.set_leader_state(am_i_leader)
        try:
            from app_config.config_loader import Config
            cfg_path = os.getenv("CONFIG_PATH", "/config/config.ini")
            cfg = Config(cfg_path)
            total_results_workers = cfg.workers.results
            
            # Build list of all results finisher nodes in the cluster
            all_nodes = [
                (i, f"results-finisher-{i}", 9600 + i)
                for i in range(total_results_workers)
            ]
            
            logging.info(f"Initializing election coordinator for results-finisher-{finisher_index} on port {election_port}")
            logging.info(f"Cluster nodes: {all_nodes}")
            
            election_coordinator = ElectionCoordinator(
                my_id=finisher_index,
                my_host="0.0.0.0",
                my_port=election_port,
                all_nodes=all_nodes,
                on_leader_change=handle_leader_change,
                election_timeout=5.0
            )
            
            heartbeat_client = HeartbeatClient(
                coordinator=election_coordinator,
                my_id=finisher_index,
                all_nodes=all_nodes,
                heartbeat_interval=heartbeat_interval,
                heartbeat_timeout=heartbeat_timeout,
                max_missed_heartbeats=heartbeat_max_misses,
                startup_grace=heartbeat_startup_grace,
                election_cooldown=heartbeat_election_cooldown,
                cooldown_jitter=heartbeat_cooldown_jitter,
            )

            node_container_map = {node_id: name for node_id, name, _ in all_nodes}
            follower_recovery = FollowerRecoveryManager(
                coordinator=election_coordinator,
                my_id=finisher_index,
                node_container_map=node_container_map,
                check_interval=max(1.0, heartbeat_interval),
                down_timeout=follower_down_timeout,
                restart_cooldown=follower_restart_cooldown,
                startup_grace=follower_recovery_grace,
            )
            
            election_coordinator.start()
            logging.info(f"Election listener started on port {election_port}")
            
            heartbeat_client.start()
            heartbeat_client.activate()
            follower_recovery.start()
            
        except Exception as e:
            logging.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
            election_coordinator = None
            heartbeat_client = None
            follower_recovery = None

        finisher = ResultsFinisher(
            input_client=input_client,
            output_client=output_client
        )
        
        shutdown_event = threading.Event()
        def shutdown_handler(signum, frame):
            logging.info("Shutdown signal received. Stopping finisher...")
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        finisher.start()
        
        logging.info("Service is running. Press Ctrl+C to exit.")
        shutdown_event.wait()
        
        logging.info("Cleaning up resources...")
        
        if election_coordinator:
            election_coordinator.graceful_resign()
            logging.info("Stopping election coordinator...")
            election_coordinator.stop()
        
        if follower_recovery:
            follower_recovery.stop()
        
        if heartbeat_client:
            logging.info("Stopping heartbeat client...")
            heartbeat_client.stop()
        
        finisher.stop()
        logging.info("Shutdown complete.")

    except MessageMiddlewareDisconnectedError as e:
        logging.critical(f"Could not connect to RabbitMQ. Aborting. Error: {e}")
    except Exception as e:
        # FIX: Use logger to correctly show exception info
        logging.critical(f"An unexpected error occurred during setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()
