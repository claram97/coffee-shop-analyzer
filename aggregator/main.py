import logging
import os
import signal
import threading
from configparser import ConfigParser

from aggregator import Aggregator
from leader_election import ElectionCoordinator, HeartbeatClient


def resolve_config_path() -> str:
    candidates = []
    env_cfg_path = os.getenv("CONFIG_PATH")
    if env_cfg_path:
        candidates.append(env_cfg_path)
    env_cfg = os.getenv("CFG")
    if env_cfg:
        candidates.append(env_cfg)
    candidates.extend(("/config/config.ini", "./config.ini", "/app_config/config.ini"))

    for path in candidates:
        if path and os.path.exists(path):
            return os.path.abspath(path)

    return os.path.abspath(candidates[0])


def initialize_config():
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
        config_params["aggregator_id"] = os.getenv(
            "AGGREGATOR_ID", config.get("DEFAULT", "AGGREGATOR_ID", fallback="0")
        )

    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e)
        )

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    aggregator_id = int(config_params["aggregator_id"])

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | "
        f"logging_level: {logging_level} | "
        f"aggregator_id: {aggregator_id}"
    )

    election_port = int(os.getenv("ELECTION_PORT", 9300 + aggregator_id))

    # Get total aggregators from config
    cfg_path = resolve_config_path()
    from app_config.config_loader import Config

    cfg = Config(cfg_path)
    total_aggregators = cfg.workers.aggregators

    stop_event = threading.Event()

    def signal_handler(signum, _frame):
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2.0"))
    heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "1.0"))
    heartbeat_max_misses = int(os.getenv("HEARTBEAT_MAX_MISSES", "3"))
    heartbeat_startup_grace = float(os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS", "4.0"))

    election_coordinator = None
    heartbeat_client = None

    def handle_leader_change(new_leader_id: int, am_i_leader: bool):
        logging.info(
            "Leader update received | new_leader=%s | am_i_leader=%s",
            new_leader_id,
            am_i_leader,
        )
        if heartbeat_client:
            if am_i_leader:
                heartbeat_client.deactivate()
            else:
                heartbeat_client.activate()

    try:
        # Build list of all aggregator nodes in the cluster
        all_nodes = [(i, f"aggregator-{i}", 9300 + i) for i in range(total_aggregators)]

        logging.info(
            f"Initializing election coordinator for aggregator-{aggregator_id} on port {election_port}"
        )
        logging.info(f"Cluster nodes: {all_nodes}")

        election_coordinator = ElectionCoordinator(
            my_id=aggregator_id,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=5.0,
        )

        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=aggregator_id,
            all_nodes=all_nodes,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            max_missed_heartbeats=heartbeat_max_misses,
            startup_grace=heartbeat_startup_grace,
        )

        election_coordinator.start()
        logging.info(f"Election listener started on port {election_port}")
        heartbeat_client.start()
        heartbeat_client.activate()

    except Exception as e:
        logging.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
        election_coordinator = None
        heartbeat_client = None

    server = Aggregator(aggregator_id)
    server.run()

    try:
        stop_event.wait()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received, shutting down...")
    finally:
        logging.info("Cleaning up resources...")

        if election_coordinator:
            election_coordinator.graceful_resign()
            logging.info("Stopping election coordinator...")
            election_coordinator.stop()
        if heartbeat_client:
            logging.info("Stopping heartbeat client...")
            heartbeat_client.stop()

        server.stop()
        logging.info("Graceful shutdown complete.")


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


if __name__ == "__main__":
    main()
