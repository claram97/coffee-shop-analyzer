# Election Port Configuration

## Overview

Added explicit port configuration for leader election to ensure each container within a component group knows where to send election messages.

## Configuration Structure

### Port Allocation
Each component group gets a base port with 100 ports reserved:

| Component Group      | Base Port | Port Range     | Max Replicas |
|---------------------|-----------|----------------|--------------|
| Filter Workers      | 9100      | 9100-9199      | 100          |
| Filter Routers      | 9200      | 9200-9299      | 100          |
| Aggregators         | 9300      | 9300-9399      | 100          |
| Joiner Workers      | 9400      | 9400-9499      | 100          |
| Joiner Routers      | 9500      | 9500-9599      | 100          |
| Results Workers     | 9600      | 9600-9699      | 100          |
| Results Routers     | 9700      | 9700-9799      | 100          |

**Note:** Orchestrator does not participate in elections and has no election port.

### Port Assignment Formula
```
container_port = base_port + replica_index
```

**Examples:**
- `filter-router-0` → port 9200
- `filter-router-1` → port 9201
- `aggregator-3` → port 9303
- `joiner-worker-5` → port 9405

## Files Modified

### 1. `app_config/config.ini`
Added new `[election_ports]` section:

```ini
[election_ports]
# Base port for leader election in each component group
# Each component gets 100 ports (base_port + replica_id)
# Note: Orchestrator does not participate in elections
filter_workers = 9100
filter_routers = 9200
aggregators = 9300
joiner_workers = 9400
joiner_routers = 9500
results_workers = 9600
results_routers = 9700
```

### 2. `app_config/config_subscript.py`
Added support for reading election ports:

- New function: `_read_election_ports()` - Reads election_ports section with defaults
- New function: `cmd_election_ports()` - Outputs election ports in plain or env format
- Updated `main()` - Added `election_ports` subcommand

**Usage:**
```bash
# Get as environment variables
python3 config_subscript.py -c config.ini election_ports --format=env

# Get as space-separated values
python3 config_subscript.py -c config.ini election_ports --format=plain
```

### 3. `generate-compose.sh`
Updated to read and inject election ports:

**Changes:**
1. Added election port reading:
   ```bash
   eval "$(python3 ./app_config/config_subscript.py -c "$INI_PATH" election_ports --format=env)"
   ```

2. Added default values for each port variable

3. Updated all component sections to include `ELECTION_PORT` environment variable:
   - Filter routers: `ELECTION_PORT=$((ELECTION_PORT_FILTER_ROUTERS + i))`
   - Filter workers: `ELECTION_PORT=$((ELECTION_PORT_FILTER_WORKERS + i))`
   - Aggregators: `ELECTION_PORT=$((ELECTION_PORT_AGGREGATORS + i))`
   - Joiner routers: `ELECTION_PORT=$((ELECTION_PORT_JOINER_ROUTERS + i))`
   - Joiner workers: `ELECTION_PORT=$((ELECTION_PORT_JOINER_WORKERS + i))`
   - Results routers: `ELECTION_PORT=$((ELECTION_PORT_RESULTS_ROUTERS + i))`
   - Results finishers: `ELECTION_PORT=$((ELECTION_PORT_RESULTS_WORKERS + i))`
   
   **Note:** Orchestrator is excluded as it does not participate in elections.

## Generated Docker Compose

Each container now receives its election port as an environment variable:

```yaml
filter-router-0:
  container_name: filter-router-0
  environment:
    - ELECTION_PORT=9200
    # ... other env vars

filter-router-1:
  container_name: filter-router-1
  environment:
    - ELECTION_PORT=9201
    # ... other env vars

aggregator-0:
  container_name: aggregator-0
  environment:
    - ELECTION_PORT=9300
    # ... other env vars
```

## Usage in Components

Components can read the `ELECTION_PORT` environment variable:

```python
import os

# Get my election port
my_port = int(os.getenv('ELECTION_PORT', '9000'))

# Get my replica ID from container name or env var
replica_id = int(os.getenv('FILTER_ROUTER_INDEX', '0'))

# Calculate ports for other replicas in my group
base_port = my_port - replica_id
other_replica_ports = [
    (i, 'container-name', base_port + i) 
    for i in range(total_replicas)
    if i != replica_id
]

# Start election coordinator
from leader_election import start_election_thread

coordinator = start_election_thread(
    my_id=replica_id,
    my_host='0.0.0.0',  # Listen on all interfaces
    my_port=my_port,
    all_nodes=other_replica_ports,
    on_leader_change=handle_leader_change
)
```

## Benefits

1. **Explicit Port Allocation** - No port conflicts, clear assignment
2. **Scalable** - 100 ports per component group supports large deployments
3. **Configurable** - Easy to change base ports in config.ini
4. **Automatic** - Generate script calculates replica ports automatically
5. **Discoverable** - Each container knows its port via environment variable

## Testing

```bash
# Generate compose file
./generate-compose.sh -c app_config/config.ini -o docker-compose-dev.yaml

# Verify election ports are set
grep "ELECTION_PORT" docker-compose-dev.yaml

# Check specific component
grep -A 12 "filter-router-0:" docker-compose-dev.yaml
```

## Port Range Summary

Total ports reserved: 700 (7 component groups × 100 ports)
Port range: 9100-9799

**Note:** Orchestrator (port range 9000-9099) is not used as it doesn't participate in elections.

This leaves plenty of room for:
- Additional component types (9800+)
- Other services
- Future expansion
