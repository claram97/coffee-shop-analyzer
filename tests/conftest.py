import os
import shutil
import tempfile

import pytest  # type: ignore[import-not-found]

from protocol.constants import Opcodes
from protocol.databatch import DataBatch
# UUID used for tests to satisfy DataBatch serialization requirements
TEST_CLIENT_ID = "00000000-0000-0000-0000-000000000001"

from protocol.messages import (
    NewMenuItems,
    NewStores,
    NewTransactionItems,
    NewTransactionItemsMenuItems,
    NewTransactions,
    NewTransactionStores,
    NewTransactionStoresUsers,
    NewUsers,
)


# ---------- helpers de framing ----------
def _db_with(inner_msg, *, table_ids, query_ids=None, batch_number=1, meta=None):
    """Construye bytes de DataBatch a partir de un TableMessage ya rellenado con .rows."""
    # El worker arma batch_bytes con inner_msg.to_bytes(), así que imitamos eso
    batch_bytes = inner_msg.to_bytes()
    db = DataBatch(
        table_ids=list(table_ids),
        query_ids=list(query_ids or []),
        shards_info=[(1, 0)],
        reserved_u16=0,
        meta=dict(meta or {}),
        batch_bytes=batch_bytes,
        client_id=TEST_CLIENT_ID,
    )
    # opcionalmente setear el batch_number para rastrear (si tu DataBatch lo guarda)
    db.batch_number = batch_number
    return db.to_bytes()


# ---------- fakes  ----------
class FakeOutQueue:
    def __init__(self):
        self.sent = []

    def send(self, raw: bytes):
        self.sent.append(raw)

    # API compatible con MessageMiddlewareQueue.close() para tests
    def close(self):
        pass


@pytest.fixture
def tmp_data_dir(tmp_path):
    d = tmp_path / "joiner-data"
    d.mkdir(parents=True, exist_ok=True)
    return str(d)  # path de string


@pytest.fixture
def fake_out():
    return FakeOutQueue()


@pytest.fixture
def empty_inputs():
    # el worker no usará start_consuming en unit tests: llamamos a _on_raw_* directos
    return {}
