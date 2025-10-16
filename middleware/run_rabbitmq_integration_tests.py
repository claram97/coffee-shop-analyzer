#!/usr/bin/env python3
"""Run integration tests against a live RabbitMQ container.

Requires `pytest` and `pika` to be installed in the current Python environment.
"""

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MIDDLEWARE_DIR = REPO_ROOT / "middleware"
COMPOSE_FILE = MIDDLEWARE_DIR / "docker-compose.yml"
DEFAULT_SERVICE = "rabbitmq"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5672
DEFAULT_TIMEOUT = 60


def run(command, **kwargs):
    result = subprocess.run(command, cwd=REPO_ROOT, check=False, **kwargs)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def wait_for_port(host: str, port: int, timeout: int) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    print(f"Could not connect to {host}:{port} within {timeout}s", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run integration tests against a live RabbitMQ container."
    )
    parser.add_argument(
        "--service",
        default=DEFAULT_SERVICE,
        help="Service name defined in middleware/docker-compose.yml",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="Hostname RabbitMQ will be reachable at (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port RabbitMQ will be reachable at (default: 5672)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Seconds to wait for RabbitMQ to become ready (default: 60)",
    )
    parser.add_argument(
        "--keep-up",
        action="store_true",
        help="Do not stop the docker-compose services after tests",
    )
    parser.add_argument(
        "--compose-command",
        default="docker-compose",
        help="Command to invoke docker compose (default: docker-compose)",
    )
    args = parser.parse_args()

    if not COMPOSE_FILE.exists():
        raise SystemExit(f"Compose file not found at {COMPOSE_FILE}")

    compose_base = [args.compose_command, "-f", str(COMPOSE_FILE)]

    try:
        run(compose_base + ["up", "-d", args.service])
        wait_for_port(args.host, args.port, timeout=args.timeout)

        env = os.environ.copy()
        env["RUN_RABBITMQ_INTEGRATION"] = "1"
        env["RABBITMQ_HOST"] = args.host

        run([
            sys.executable,
            "-m",
            "pytest",
            "tests/test_middleware_client_integration.py",
        ], env=env)
    finally:
        if not args.keep_up:
            run(compose_base + ["down"])


if __name__ == "__main__":
    main()
