import subprocess
from pathlib import Path

from topology import Topology

DOCKER_COMPOSE_ONE_TO_ONE_YAML = """
version: "3.8"
services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=guest
      - RABBITMQ_DEFAULT_PASS=guest
      - RABBITMQ_DEFAULT_VHOST=/
    networks:
      - testing_net
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5

  orchestrator:
    container_name: orchestrator
    build:
      context: .
      dockerfile: orchestrator/Dockerfile
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS_AMOUNT=1
      - RABBITMQ_HOST=rabbitmq
      - ORCH_TO_FR_EXCHANGE=fr.ex
      - ORCH_TO_FR_RK_FMT=fr.{pid:02d}
      - FILTER_ROUTER_COUNT=1
    networks:
      - testing_net
    volumes:
      - ./orchestrator/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-0:
    container_name: filter-router-0
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-worker-0:
    container_name: filter-worker-0
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  aggregator-0:
    container_name: aggregator-0
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  joiner-router-0:
    container_name: joiner-router-0
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started

  joiner-worker-0:
    container_name: joiner-worker-0
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=0
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  results-router-0:
    container_name: results-router-0
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=finisher_input_queue_0
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  results-finisher-0:
    container_name: results-finisher-0
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only
      - INPUT_QUEUE=finisher_input_queue_0
      - OUTPUT_QUEUE=orchestrator_results_queue
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

DOCKER_COMPOSE_ONE_TO_MANY_YAML = """
version: '3.8'
services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=guest
      - RABBITMQ_DEFAULT_PASS=guest
      - RABBITMQ_DEFAULT_VHOST=/
    networks:
      - testing_net
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5

  orchestrator:
    container_name: orchestrator
    build:
      context: .
      dockerfile: orchestrator/Dockerfile
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS_AMOUNT=1
      - RABBITMQ_HOST=rabbitmq
      - ORCH_TO_FR_EXCHANGE=fr.ex
      - ORCH_TO_FR_RK_FMT=fr.{pid:02d}
      - FILTER_ROUTER_COUNT=6
    networks:
      - testing_net
    volumes:
      - ./orchestrator/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-0:
    container_name: filter-router-0
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-1:
    container_name: filter-router-1
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=1
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-2:
    container_name: filter-router-2
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=2
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-3:
    container_name: filter-router-3
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=3
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-4:
    container_name: filter-router-4
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=4
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-router-5:
    container_name: filter-router-5
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=5
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  filter-worker-0:
    container_name: filter-worker-0
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-1:
    container_name: filter-worker-1
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-2:
    container_name: filter-worker-2
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-3:
    container_name: filter-worker-3
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-4:
    container_name: filter-worker-4
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-5:
    container_name: filter-worker-5
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-6:
    container_name: filter-worker-6
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  filter-worker-7:
    container_name: filter-worker-7
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  aggregator-0:
    container_name: aggregator-0
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  aggregator-1:
    container_name: aggregator-1
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=1
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  aggregator-2:
    container_name: aggregator-2
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=2
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  aggregator-3:
    container_name: aggregator-3
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=3
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started

  joiner-router-0:
    container_name: joiner-router-0
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=0
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started

  joiner-router-1:
    container_name: joiner-router-1
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=1
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started

  joiner-router-2:
    container_name: joiner-router-2
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=2
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started

  joiner-router-3:
    container_name: joiner-router-3
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=3
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started

  joiner-worker-0:
    container_name: joiner-worker-0
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=0
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  joiner-worker-1:
    container_name: joiner-worker-1
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=1
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  joiner-worker-2:
    container_name: joiner-worker-2
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=2
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  joiner-worker-3:
    container_name: joiner-worker-3
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=3
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  joiner-worker-4:
    container_name: joiner-worker-4
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=4
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  joiner-worker-5:
    container_name: joiner-worker-5
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=5
      - CONFIG_PATH=/config/config.ini
    networks:
      - testing_net
    volumes:
      - ./integration_tests/sys_config.ini:/config/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started

  results-router-0:
    container_name: results-router-0
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=finisher_input_queue_0,finisher_input_queue_1,finisher_input_queue_2,finisher_input_queue_3
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  results-router-1:
    container_name: results-router-1
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=finisher_input_queue_0,finisher_input_queue_1,finisher_input_queue_2,finisher_input_queue_3
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  results-router-2:
    container_name: results-router-2
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=finisher_input_queue_0,finisher_input_queue_1,finisher_input_queue_2,finisher_input_queue_3
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  results-router-3:
    container_name: results-router-3
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=finisher_input_queue_0,finisher_input_queue_1,finisher_input_queue_2,finisher_input_queue_3
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  results-finisher-0:
    container_name: results-finisher-0
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only  # Can be 'append_only' or 'incremental'
      - INPUT_QUEUE=finisher_input_queue_0
      - OUTPUT_QUEUE=orchestrator_results_queue
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started

  results-finisher-1:
    container_name: results-finisher-1
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only  # Can be 'append_only' or 'incremental'
      - INPUT_QUEUE=finisher_input_queue_1
      - OUTPUT_QUEUE=orchestrator_results_queue
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started

  results-finisher-2:
    container_name: results-finisher-2
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only  # Can be 'append_only' or 'incremental'
      - INPUT_QUEUE=finisher_input_queue_2
      - OUTPUT_QUEUE=orchestrator_results_queue
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started

  results-finisher-3:
    container_name: results-finisher-3
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only  # Can be 'append_only' or 'incremental'
      - INPUT_QUEUE=finisher_input_queue_3
      - OUTPUT_QUEUE=orchestrator_results_queue
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""


def _get_project_root() -> Path:
    """Calculates the project root directory."""
    try:
        script_dir = Path(__file__).parent
        project_root = (script_dir / "..").resolve()
    except NameError:
        project_root = Path("..").resolve()
    return project_root


def _run_compose_command(
    base_command: list[str], project_root: Path, compose_filename: str
):
    """
    A helper function to execute a docker-compose command with a specific file.
    """
    # Command is constructed with -f to specify the compose file
    command = ["docker", "compose", "-f", compose_filename] + base_command
    print(f"\nRunning '{' '.join(command)}' in '{project_root}'...")
    try:
        result = subprocess.run(
            command, cwd=project_root, check=True, capture_output=True, text=True
        )
        print("Docker Compose command completed successfully.")
        print("--- Docker Compose Output ---")
        print(result.stdout)
    except FileNotFoundError:
        print("Error: 'docker-compose' not found. Is it installed and in your PATH?")
        raise
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the command.")
        print(
            f"Return Code: {e.returncode}\n--- STDOUT ---\n{e.stdout}\n--- STDERR ---\n{e.stderr}"
        )
        raise


def start_services(topology: Topology, project_root: Path | None = None):
    """Writes docker-compose-one-to-one.yml and starts the services."""
    if project_root is None:
        project_root = _get_project_root()
    compose_filename = topology.value
    compose_file_path = project_root / compose_filename

    print(f"Writing {compose_filename} to: {compose_file_path}")
    try:
        with open(compose_file_path, "w") as f:
            f.write(DOCKER_COMPOSE_ONE_TO_ONE_YAML)
        print("File created successfully.")
    except IOError as e:
        print(f"Error writing to file {compose_file_path}: {e}")
        raise

    _run_compose_command(["up", "--build", "-d"], project_root, str(compose_filename))


def stop_services(topology: Topology, project_root: Path | None = None):
    """Stops services and deletes docker-compose-one-to-one.yml."""
    if project_root is None:
        project_root = _get_project_root()
    compose_filename = topology.value
    compose_file_path = project_root / compose_filename

    if compose_file_path.exists():
        _run_compose_command(["down"], project_root, compose_filename)
        try:
            print(f"\nRemoving {compose_filename}...")
            compose_file_path.unlink()
            print("File removed successfully.")
        except OSError as e:
            print(f"Error removing file {compose_file_path}: {e}")
    else:
        print(f"{compose_filename} not found. Skipping 'down' and deletion.")
