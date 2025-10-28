# Delayed ACK Implementation for EOF Fault Tolerance

## Overview
This implementation makes the filter-router fault-tolerant by delaying ACK for EOF messages until they are fully processed. This leverages RabbitMQ's built-in message redelivery mechanism to recover state after crashes.

## Changes Made

### 1. `filter/router.py` - FilterRouter class

#### Added state tracking for unacked EOFs:
```python
self._unacked_eofs: Dict[tuple[TableName, str], tuple] = {}
self._eof_ack_lock = threading.Lock()
```

#### Modified `process_message()`:
- Now accepts `channel`, `delivery_tag`, and `redelivered` parameters
- Returns `(should_ack, ack_now)` tuple:
  - `should_ack`: Always True (message should be acked eventually)
  - `ack_now`: True for data batches, False for EOFs (delay ACK)

#### Modified `_handle_table_eof()`:
- Now accepts `channel`, `delivery_tag`, `redelivered` parameters
- Stores (channel, delivery_tag) in `_unacked_eofs` dictionary
- Logs when redelivered EOFs are received (restart scenario)
- Returns `(True, False)` to signal delayed ACK

#### Added `_ack_eof()` method:
- Called after EOF is forwarded to all aggregators
- Retrieves (channel, delivery_tag) from `_unacked_eofs`
- Calls `channel.basic_ack(delivery_tag)` to acknowledge the message
- Thread-safe with `_eof_ack_lock`

#### Modified `_maybe_flush_pending_eof()`:
- Added call to `_ack_eof(key)` after successfully forwarding EOF

### 2. `filter/router.py` - RouterServer class

#### Modified `run()` callback:
- Updated callback to accept and pass through: `channel`, `delivery_tag`, `redelivered`
- Returns `ack_now` to middleware to control ACK behavior
- Returns `True` on shutdown or errors to avoid infinite redelivery

### 3. `middleware/middleware_client.py` - Both Queue and Exchange classes

#### Modified `_create_callback_wrapper()`:
- Passes `channel`, `delivery_tag`, `redelivered` to user callback
- Checks callback return value:
  - `True` or `None`: ACK immediately (default behavior)
  - `False`: Don't ACK (manual ACK required)
- Logs whether ACK is immediate or delayed

#### Updated prefetch_count:
- Changed from 3 to 10 to allow multiple unacked messages
- Necessary because EOFs can stay unacked while waiting for all orch workers

## How It Works

### Normal Operation Flow:

1. **EOF arrives from orchestrator:**
   - FilterRouter stores `(channel, delivery_tag)` in `_unacked_eofs`
   - Returns `(True, False)` → middleware doesn't ACK
   - EOF remains in "unacked" state in RabbitMQ

2. **Data batches arrive:**
   - Processed normally
   - Returns `(True, True)` → middleware ACKs immediately
   - No change to existing behavior

3. **All EOFs received from orch workers:**
   - `_maybe_flush_pending_eof()` triggers
   - Forwards EOF to all aggregators
   - Calls `_ack_eof(key)` which ACKs the original message
   - EOF is removed from queue

### Crash Recovery Flow:

1. **Filter-router crashes:**
   - Connection to RabbitMQ is lost
   - RabbitMQ detects closed connection
   - All unacked messages (EOFs) are automatically requeued

2. **Filter-router restarts:**
   - Reconnects to RabbitMQ
   - Receives redelivered EOFs with `redelivered=True`
   - Logs: "TABLE_EOF REDELIVERED (recovering state)"
   - Processes EOFs normally, rebuilding state
   - Forwards to aggregators and ACKs

## Key Benefits

✅ **Zero new components** - Uses existing RabbitMQ infrastructure
✅ **Simple implementation** - ~50 lines of code changes
✅ **Automatic recovery** - RabbitMQ handles redelivery
✅ **Idempotent** - Can handle redelivered EOFs safely
✅ **Thread-safe** - Uses locks for concurrent access
✅ **No external storage** - State is "in-flight" in RabbitMQ

## Testing

### Manual Testing:
1. Start system normally
2. Send data from client
3. Kill filter-router process: `docker kill filter-router-0`
4. Check logs for EOFs being redelivered on restart
5. Verify system completes correctly

### Expected Log Output:

**Normal EOF:**
```
TABLE_EOF received: key=(TableName.MENU_ITEMS, 'client1')
TABLE_EOF deferred: key=(TableName.MENU_ITEMS, 'client1') pending=5
TABLE_EOF -> aggregators: key=(TableName.MENU_ITEMS, 'client1') parts=4
ACKing TABLE_EOF: key=(TableName.MENU_ITEMS, 'client1') delivery_tag=12345
```

**Redelivered EOF (after restart):**
```
TABLE_EOF REDELIVERED (recovering state): key=(TableName.MENU_ITEMS, 'client1')
TABLE_EOF -> aggregators: key=(TableName.MENU_ITEMS, 'client1') parts=4
ACKing TABLE_EOF: key=(TableName.MENU_ITEMS, 'client1') delivery_tag=67890
```

## Compatibility

### Backward Compatible Changes:
- Middleware now accepts optional kwargs in callbacks
- Old callbacks that don't use the new parameters still work
- Return value of `None` or no return is treated as `True` (ACK immediately)

### No Changes Required:
- Other components (orchestrator, aggregator, etc.) unchanged
- Filter workers unchanged
- Protocol unchanged
- Configuration unchanged

## Edge Cases Handled

1. **Multiple restarts:** EOFs can be redelivered multiple times, each time rebuilding state
2. **Partial forwarding:** If some aggregators receive EOF but router crashes, redelivery ensures all get it
3. **Concurrent EOFs:** Thread-safe with locks, supports multiple client_ids simultaneously
4. **Shutdown during processing:** Returns `True` on shutdown to avoid redelivery of messages being processed

## Future Improvements (Optional)

- Add metrics for unacked EOF count
- Add timeout for unacked EOFs (alert if stuck)
- Add explicit state persistence to disk for faster recovery
- Extend to other message types if needed
