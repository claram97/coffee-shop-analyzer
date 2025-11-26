import json
import sys
import time
from enum import Enum
from pathlib import Path

try:
    import initiate_client
    import initiate_system
    from topology import Topology
except ImportError as e:
    print(
        f"Error: Could not import required modules. Make sure the file names are correct."
    )
    print(f"Details: {e}")
    sys.exit(1)


# --- Test Configuration ---
PROJECT_ROOT = Path(__file__).parent.resolve() / ".."
RESULTS_DIR = PROJECT_ROOT / "integration_tests"
QUERIES = [
    "query1",
    "query2",
    "query3",
    "query4",
]
RESULT_FILES = [RESULTS_DIR / f"{q}_results.json" for q in QUERIES]
EXPECTED_FILES = [RESULTS_DIR / f"{q}_results_expected.json" for q in QUERIES]

# Polling configuration
WAIT_TIMEOUT_SECONDS = 300  # 5 minutes
POLL_INTERVAL_SECONDS = 5


def wait_for_results() -> bool:
    """
    Waits for all result files to exist and be non-empty.
    Checks in client_runs subdirectories since client writes there.

    Returns:
        bool: True if results are ready, False if timed out.
    """
    print(f"\nWaiting for results... (Timeout: {WAIT_TIMEOUT_SECONDS}s)")
    start_time = time.time()

    client_runs_dir = RESULTS_DIR / "client_runs"

    while time.time() - start_time < WAIT_TIMEOUT_SECONDS:
        try:
            # Find the client directory (should be a UUID)
            if not client_runs_dir.exists():
                print(
                    f"  ({int(time.time() - start_time)}s elapsed) client_runs directory doesn't exist yet..."
                )
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            client_dirs = [d for d in client_runs_dir.iterdir() if d.is_dir()]
            if not client_dirs:
                print(
                    f"  ({int(time.time() - start_time)}s elapsed) No client directory found yet..."
                )
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Use the first (should be only) client directory
            client_dir = client_dirs[0]
            print(f"  Found client directory: {client_dir.name}")

            ready_files = 0
            for query in QUERIES:
                file_path = client_dir / f"{query}_results.json"
                if file_path.exists() and file_path.stat().st_size > 0:
                    ready_files += 1

            if ready_files == len(QUERIES):
                print(f"All result files are available in {client_dir.name}.")
                return True
        except FileNotFoundError:
            pass

        print(
            f"  ({int(time.time() - start_time)}s elapsed) {ready_files}/{len(QUERIES)} results ready, checking again in {POLL_INTERVAL_SECONDS}s..."
        )
        time.sleep(POLL_INTERVAL_SECONDS)

    print("\nError: Timed out waiting for result files.")
    return False


def check_results() -> dict[str, str]:
    """
    Compares the contents of result files with their expected counterparts.

    Returns:
        dict[str, str]: A dictionary with the pass/fail status for each query.
    """
    print("\nChecking results...")
    test_outcomes = {}

    # Find the client directory
    client_runs_dir = RESULTS_DIR / "client_runs"
    client_dirs = [d for d in client_runs_dir.iterdir() if d.is_dir()]
    if not client_dirs:
        print("ERROR: No client directory found!")
        for query in QUERIES:
            test_outcomes[query] = "FAIL (No client directory)"
        return test_outcomes

    client_dir = client_dirs[0]
    print(f"Checking results in: {client_dir.name}")

    for i, query in enumerate(QUERIES):
        result_file = client_dir / f"{query}_results.json"
        expected_file = EXPECTED_FILES[i]

        try:
            with open(result_file, "r") as f_res, open(expected_file, "r") as f_exp:
                result_data = json.load(f_res)
                expected_data = json.load(f_exp)

                if result_data == expected_data:
                    test_outcomes[query] = "PASS"
                    print(f"  - {query}: PASS")
                else:
                    test_outcomes[query] = "FAIL"
                    print(f"  - {query}: FAIL (Contents do not match)")
        except FileNotFoundError as e:
            test_outcomes[query] = "FAIL (File not found)"
            print(f"  - {query}: FAIL ({e.filename} not found)")
        except json.JSONDecodeError as e:
            test_outcomes[query] = f"FAIL (Invalid JSON in {e.doc})"
            print(f"  - {query}: FAIL (Invalid JSON: {e})")

    return test_outcomes


def clear_results_files():
    """Clears the client_runs directory using Docker to handle root-owned files."""
    print("\nClearing result files...")
    client_runs_dir = RESULTS_DIR / "client_runs"
    try:
        if client_runs_dir.exists() and list(client_runs_dir.iterdir()):
            import subprocess
            
            # Use a temporary docker container to remove root-owned files
            # This avoids needing sudo and works because docker can access root files
            print(f"  - Using Docker to clear {client_runs_dir} (contains root-owned files)")
            subprocess.run(
                [
                    "docker", "run", "--rm",
                    "-v", f"{client_runs_dir.absolute()}:/cleanup",
                    "busybox",
                    "sh", "-c", "rm -rf /cleanup/* /cleanup/.*"
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            print(f"  - Cleared {client_runs_dir}")
        
        # Ensure directory exists
        client_runs_dir.mkdir(parents=True, exist_ok=True)
        print(f"  - Cleaned up {client_runs_dir}")
    except Exception as e:
        print(f"  - Warning: Could not clear {client_runs_dir}. Reason: {e}")
        print(f"  - You can manually remove with: docker run --rm -v $(pwd)/integration_tests/client_runs:/cleanup busybox sh -c 'rm -rf /cleanup/*'")


def run_test():
    """Executes the full integration test flow."""
    results = {}
    services_started = False
    try:
        # 1. & 2. Start services
        print("--- Starting all services ---")
        initiate_system.start_services(
            topology=Topology.MANY_TO_MANY,
            project_root=PROJECT_ROOT,
        )
        time.sleep(10)
        initiate_client.start_docker_services(project_root=PROJECT_ROOT)
        services_started = True
        print("All services initiated.")

        # 3. Wait for results
        if not wait_for_results():
            raise RuntimeError("Test failed due to timeout.")

        # 4. Check results
        results = check_results()

    except Exception as e:
        print(f"\n--- An error occurred during the test ---")
        print(e)
        results["overall_status"] = "ERROR"
    finally:
        # 5. Clear results (done after check, before stop)
        if services_started:
            clear_results_files()

        # 6. Stop all services
        print("\n--- Stopping all services ---")
        # Stop in reverse order of start
        if services_started:
            initiate_client.stop_docker_services(project_root=PROJECT_ROOT)
            initiate_system.stop_services(
                project_root=PROJECT_ROOT, topology=Topology.MANY_TO_MANY
            )
        print("Cleanup complete.")

        # 7. Print final results
        print("\n--- TEST SUMMARY ---")
        if not results:
            print("No results were generated due to an early error.")
        for test, outcome in results.items():
            print(f"{test.upper():<15} | {outcome}")
        print("--------------------")


if __name__ == "__main__":
    run_test()
