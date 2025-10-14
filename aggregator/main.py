import logging
import os
import signal
import time
from configparser import ConfigParser

from aggregator import Aggregator


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
    aggregator_id = config_params["aggregator_id"]

    initialize_log(logging_level)

    logging.debug(
        f"action: config | result: success | "
        f"logging_level: {logging_level} | "
        f"aggregator_id: {aggregator_id}"
    )

    def signal_handler(signum, _frame):
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        if 'server' in locals():
            server.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    server = Aggregator(aggregator_id)
    server.run()
    signal.pause()


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
