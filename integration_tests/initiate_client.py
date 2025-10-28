import subprocess
from pathlib import Path

# Define the content for your docker-compose.yml file
DOCKER_COMPOSE_YAML = """
version: "3.8"
services:
  client1:
    container_name: client1
    build:
      context: .
      dockerfile: client/Dockerfile
    entrypoint: /client
    environment:
      - CLI_ID=1
    networks:
      - testing_net
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./integration_tests/test_data:/data:ro
      - ./integration_tests/client_runs:/client_runs

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""


def _get_project_root() -> Path:
    """
    Calculates the project root directory, assuming the script is in a subfolder.
    """
    try:
        script_dir = Path(__file__).parent
        project_root = (script_dir / "..").resolve()
    except NameError:
        project_root = Path("..").resolve()
    return project_root


def _run_compose_command(command: list[str], project_root: Path):
    """
    A helper function to execute a docker-compose command.

    Args:
        command (list[str]): The docker-compose command to run as a list of strings.
        project_root (Path): The absolute path to the project's root directory.
    """
    print(f"\nRunning '{' '.join(command)}' in '{project_root}'...")
    try:
        result = subprocess.run(
            command, cwd=project_root, check=True, capture_output=True, text=True
        )
        print("Docker Compose command completed successfully.")
        print("--- Docker Compose Output ---")
        print(result.stdout)

    except FileNotFoundError:
        print(
            "Error: 'docker-compose' command not found. "
            "Please ensure Docker and Docker Compose are installed and in your system's PATH."
        )
        raise
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the command.")
        print(f"Return Code: {e.returncode}")
        print("\n--- STDOUT ---")
        print(e.stdout)
        print("\n--- STDERR ---")
        print(e.stderr)
        raise


def start_docker_services(project_root: Path | None = None):
    """
    Writes the docker-compose.yml file and starts the services.

    Args:
        project_root (Path | None, optional): The path to the project root.
                                              If None, it's inferred. Defaults to None.
    """
    if project_root is None:
        project_root = _get_project_root()

    compose_file_path = project_root / "docker-compose-client-test.yml"

    print(f"Writing docker-compose-client-test.yml to: {compose_file_path}")
    try:
        with open(compose_file_path, "w") as f:
            f.write(DOCKER_COMPOSE_YAML)
        print("File created successfully.")
    except IOError as e:
        print(f"Error: Could not write to file {compose_file_path}. {e}")
        raise

    command = ["docker", "compose", "-f", str(compose_file_path), "up", "--build", "-d"]
    _run_compose_command(command, project_root)


def stop_docker_services(project_root: Path | None = None):
    """
    Stops, removes the docker-compose services, and deletes the docker-compose.yml file.

    Args:
        project_root (Path | None, optional): The path to the project root.
                                              If None, it's inferred. Defaults to None.
    """
    if project_root is None:
        project_root = _get_project_root()
    compose_file_path = project_root / "docker-compose-client-test.yml"
    if compose_file_path.exists():
        command = ["docker", "compose", "-f", str(compose_file_path), "down"]
        _run_compose_command(command, project_root)
        try:
            print(
                f"\nRemoving docker-compose-client-test.yml file from: {compose_file_path}"
            )
            compose_file_path.unlink()
            print("File removed successfully.")
        except OSError as e:
            print(f"Error: Could not remove file {compose_file_path}. {e}")
    else:
        print(
            "docker-compose-client-test.yml not found. Skipping 'down' command and file deletion."
        )


if __name__ == "__main__":
    print("--- Example Usage ---")
    try:
        print("\n>>> Starting Docker services...")
        start_docker_services()

        input("\nPress Enter to stop and remove the services...")

        print("\n>>> Stopping Docker services...")
        stop_docker_services()
    except Exception as e:
        print(f"\nAn operation failed: {e}")
