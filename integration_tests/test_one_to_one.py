import json
import sys
import time
from enum import Enum
from pathlib import Path

# --- Assumptions ---
# 1. This script is in a subdirectory (e.g., 'scripts') of the project root.
# 2. 'manage_docker.py' has been renamed to 'instantiate_client.py'.
# 3. 'initiate_one_to_one.py' has been renamed to 'initiate_system.py'.
# 4. The start_services function in 'initiate_system.py' has been modified
#    to accept a 'topology' argument as specified.

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

    Returns:
        bool: True if results are ready, False if timed out.
    """
    print(f"\nWaiting for results... (Timeout: {WAIT_TIMEOUT_SECONDS}s)")
    start_time = time.time()

    while time.time() - start_time < WAIT_TIMEOUT_SECONDS:
        try:
            ready_files = 0
            for file_path in RESULT_FILES:
                if file_path.exists() and file_path.stat().st_size > 0:
                    ready_files += 1
                else:
                    # Optional: print which file is not ready
                    # print(f"  - Waiting for: {file_path.name}")
                    pass

            if ready_files == len(RESULT_FILES):
                print("All result files are available.")
                return True
        except FileNotFoundError:
            # This can happen if the directory isn't created yet
            pass

        print(
            f"  ({int(time.time() - start_time)}s elapsed) Not all results ready, checking again in {POLL_INTERVAL_SECONDS}s..."
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
    for i, query in enumerate(QUERIES):
        result_file = RESULT_FILES[i]
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
    """Truncates all result files to zero bytes."""
    print("\nClearing result files...")
    for file_path in RESULT_FILES:
        try:
            if file_path.exists():
                with open(file_path, "w") as f:
                    pass
                print(f"  - Cleared {file_path.name}")
        except IOError as e:
            print(f"  - Warning: Could not clear {file_path.name}. Reason: {e}")


def run_test():
    """Executes the full integration test flow."""
    results = {}
    services_started = False
    try:
        # 1. & 2. Start services
        print("--- Starting all services ---")
        initiate_system.start_services(
            topology=Topology.ONE_TO_ONE, project_root=PROJECT_ROOT
        )
        time.sleep(5)
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
                project_root=PROJECT_ROOT, topology=Topology.ONE_TO_ONE
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
