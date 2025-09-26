# Orchestrator Modular Architecture

This document describes the new modular architecture for the coffee shop analyzer orchestrator.

## Architecture Overview

The orchestrator has been refactored from a monolithic design into a modular architecture with clear separation of concerns:

```
orchestrator/
├── app/
│   ├── net.py          # Legacy monolithic implementation
│   ├── net_modular.py  # New modular implementation
│   └── protocol.py     # Legacy protocol (keep for backward compatibility)
├── common/             # New modular components
│   ├── protocol/       # Message handling and communication definitions
│   ├── network/        # Connection and socket management  
│   └── processing/     # Data filtering and batch processing
├── main.py            # Legacy main (uses net.py)
├── main_modular.py    # New main (supports both architectures)
└── demo_modular.py    # Demonstration script
```

## Module Breakdown

### `common/protocol/` - Protocol Layer
Handles message definitions, parsing, and serialization:

- `constants.py` - Protocol constants, opcodes, and exceptions
- `entities.py` - Raw data classes (RawMenuItems, RawStore, etc.)
- `parsing.py` - Low-level binary parsing utilities
- `messages.py` - High-level message classes (TableMessage, NewMenuItems, etc.)
- `databatch.py` - Complex DataBatch message implementation
- `dispatcher.py` - Message routing and instantiation

### `common/network/` - Network Layer
Manages connections and message handling:

- `connection.py` - ConnectionManager and ServerManager classes
- `message_handling.py` - MessageHandler and ResponseHandler classes

### `common/processing/` - Processing Layer
Handles data transformation and filtering:

- `filters.py` - Column filtering functions for each table type
- `serialization.py` - Binary serialization of filtered data
- `batch_processor.py` - BatchProcessor class and main processing logic
- `file_utils.py` - MessageLogger for debugging and file output

## Benefits

### Maintainability
- **Single Responsibility**: Each module has one clear purpose
- **Loose Coupling**: Modules depend on well-defined interfaces
- **Easy Testing**: Individual components can be unit tested in isolation

### Extensibility
- **Pluggable Components**: Easy to swap implementations (e.g., async networking)
- **New Message Types**: Add new messages by extending base classes
- **Custom Filters**: Register new filtering logic without touching core code

### Debugging
- **Focused Logging**: Each module logs its specific domain
- **Modular Testing**: Test individual components separately
- **Clear Stack Traces**: Errors are isolated to specific modules

## Usage

### Using the Modular Architecture

1. **Configuration**: Set `USE_MODULAR_ARCHITECTURE=true` in `config.ini`

2. **Run**: `python main_modular.py`

3. **Custom Processing**: Register custom message processors:
```python
from common.network import MessageHandler
from common.protocol import Opcodes

handler = MessageHandler()
handler.register_processor(Opcodes.NEW_MENU_ITEMS, my_custom_processor)
```

### Using the Legacy Architecture

1. **Configuration**: Set `USE_MODULAR_ARCHITECTURE=false` in `config.ini`
2. **Run**: `python main_modular.py` or `python main.py`

## Example Usage

See `demo_modular.py` for examples of how to use each module:

```bash
cd orchestrator
python demo_modular.py
```

## Migration Guide

### For Existing Code
The modular architecture maintains backward compatibility:

- Legacy `app.protocol` imports still work
- `Orchestrator` class behavior is unchanged
- Configuration options are the same

### For New Development
Use the new modular imports:

```python
# Instead of: from app.protocol import NewMenuItems
from common.protocol import NewMenuItems

# Instead of: from app.protocol import create_filtered_data_batch  
from common.processing import create_filtered_data_batch
```

## Testing

Each module can be tested independently:

```bash
# Test protocol parsing
python -m pytest tests/test_protocol/

# Test network handling  
python -m pytest tests/test_network/

# Test data processing
python -m pytest tests/test_processing/
```

## Performance

The modular architecture has minimal performance overhead:

- **Import Time**: Slightly increased due to module structure
- **Runtime**: No performance difference - same algorithms
- **Memory**: Comparable memory usage
- **Network**: Identical protocol and serialization

## Future Enhancements

The modular design enables future improvements:

- **Async Networking**: Replace synchronous sockets with asyncio
- **Message Queues**: Add RabbitMQ/Kafka integration in processing layer
- **Metrics**: Add monitoring without touching business logic
- **Multiple Protocols**: Support different protocol versions
- **Load Balancing**: Distribute processing across multiple instances