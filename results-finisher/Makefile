# Build all docker images defined in docker-compose.yml
build:
	@echo "--- Building results layer images... ---"
	docker-compose build

# Start all main services (router, finishers) in the background
up:
	@echo "--- Starting results layer services... ---"
	docker-compose up -d

# Stop and remove all services and volumes
down:
	@echo "--- Stopping and removing all services and volumes... ---"
	docker-compose down -v

# View the logs of the main running services
logs:
	@echo "--- Tailing logs (Ctrl+C to stop)... ---"
	docker-compose logs -f --tail=100

# The new, more robust test command
test:
	@echo "--- (1/3) Building and starting test environment... ---"
	# Start all services in the 'test' profile (including the test_runner)
	docker-compose --profile test up -d --build
	
	@echo "\n--- (2/3) Tailing logs from the test_runner. Press Ctrl+C when test is finished. ---"
	# Attach to the logs of just the test_runner to see the pytest output
	docker-compose logs -f test_runner
	
	@echo "\n--- (3/3) Test finished. Cleaning up the environment... ---"
	# Stop and remove all containers used for the test
	docker-compose down -v