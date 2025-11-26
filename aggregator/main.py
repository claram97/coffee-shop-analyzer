import logging
import os
import signal
import sys
import threading
from configparser import ConfigParser

from aggregator import Aggregator
from leader_election import (
    ElectionCoordinator,
    HeartbeatClient,
    FollowerRecoveryManager,
)


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

    logging.info(
        f"action: config | result: success | "
        f"logging_level: {logging_level} | "
        f"aggregator_id: {aggregator_id}"
    )

    # Get total aggregators from config
    cfg_path = resolve_config_path()
    from app_config.config_loader import Config

    cfg = Config(cfg_path)
    total_aggregators = cfg.workers.aggregators
    agg_port_base = cfg.election_ports.aggregators
    election_port = int(os.getenv("ELECTION_PORT", agg_port_base + aggregator_id))

    stop_event = threading.Event()

    def signal_handler(signum, _frame):
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "1.0"))
    heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "1.0"))
    heartbeat_max_misses = int(os.getenv("HEARTBEAT_MAX_MISSES", "3"))
    heartbeat_startup_grace = float(os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS", "5.0"))
    heartbeat_election_cooldown = float(os.getenv("HEARTBEAT_ELECTION_COOLDOWN_SECONDS", "10.0"))
    heartbeat_cooldown_jitter = float(os.getenv("HEARTBEAT_COOLDOWN_JITTER_SECONDS", "0.5"))
    follower_down_timeout = float(os.getenv("FOLLOWER_DOWN_TIMEOUT_SECONDS", "10.0"))
    follower_restart_cooldown = float(os.getenv("FOLLOWER_RESTART_COOLDOWN_SECONDS", "30.0"))
    follower_recovery_grace = float(os.getenv("FOLLOWER_RECOVERY_GRACE_SECONDS", "6.0"))
    follower_max_restart_attempts = int(os.getenv("FOLLOWER_MAX_RESTART_ATTEMPTS", "100"))

    election_coordinator = None
    heartbeat_client = None
    follower_recovery = None

    def handle_leader_change(new_leader_id: int, am_i_leader: bool):
        logging.debug(
            "Leader update received | new_leader=%s | am_i_leader=%s",
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
        # Build list of all aggregator nodes in the cluster
        all_nodes = [(i, f"aggregator-{i}", agg_port_base + i) for i in range(total_aggregators)]

        logging.debug(
            f"Initializing election coordinator for aggregator-{aggregator_id} on port {election_port}"
        )
        logging.debug(f"Cluster nodes: {all_nodes}")

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
            election_cooldown=heartbeat_election_cooldown,
            cooldown_jitter=heartbeat_cooldown_jitter,
        )

        node_container_map = {node_id: name for node_id, name, _ in all_nodes}
        follower_recovery = FollowerRecoveryManager(
            coordinator=election_coordinator,
            my_id=aggregator_id,
            node_container_map=node_container_map,
            check_interval=max(1.0, heartbeat_interval),
            down_timeout=follower_down_timeout,
            restart_cooldown=follower_restart_cooldown,
            startup_grace=follower_recovery_grace,
            max_restart_attempts=follower_max_restart_attempts,
        )

        election_coordinator.start()
        logging.debug(f"Election listener started on port {election_port}")
        heartbeat_client.start()
        heartbeat_client.activate()
        follower_recovery.start()

    except Exception as e:
        logging.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None

    if not (election_coordinator and heartbeat_client and follower_recovery):
        logging.critical("Leader election components failed to start, aborting.")
        sys.exit(1)

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
        if follower_recovery:
            follower_recovery.stop()
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
