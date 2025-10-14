import pytest
from conftest import TEST_CLIENT_ID

from joiner.worker import JoinerWorker
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawStore, RawTransaction
from protocol.messages import NewStores, NewTransactions


def _mk_stores_db():
    ms = NewStores()
    ms.rows = [
        RawStore(
            store_id="st-1",
            store_name="Main St",
            street="Main 123",
            postal_code="1000",
            city="Springfield",
            state="SP",
            latitude="0",
            longitude="0",
        )
    ]
    from tests.conftest import _db_with

    return _db_with(ms, table_ids=[Opcodes.NEW_STORES])


def _mk_tx_db_q3():
    tx = NewTransactions()
    tx.rows = [
        RawTransaction(
            transaction_id="tx-10",
            store_id="st-1",
            payment_method_id="pm-1",
            user_id="u-1",
            original_amount="10.00",
            discount_applied="0.00",
            final_amount="10.00",
            created_at="2025-02-01T10:00:00Z",
            voucher_id="v-1",
        )
    ]
    from tests.conftest import _db_with

    return _db_with(tx, table_ids=[Opcodes.NEW_TRANSACTION], query_ids=[3])


def test_q3_join_tx_with_store(tmp_data_dir, fake_out, empty_inputs):
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
        router_replicas=1,
    )

    # 1) cachear stores
    worker._on_raw_stores(_mk_stores_db())

    # 2) Enablear fase de transactions
    worker._on_table_eof(Opcodes.NEW_STORES, TEST_CLIENT_ID)
    worker._on_table_eof(Opcodes.NEW_MENU_ITEMS, TEST_CLIENT_ID)

    # 3) mandar transactions con Q3
    worker._on_raw_tx(_mk_tx_db_q3())

    # 4) debería salir un batch con NEW_TRANSACTION_STORES
    assert len(fake_out.sent) == 1, "nothing sent"
    out_db = DataBatch.deserialize_from_bytes(fake_out.sent[0])
    assert out_db.table_ids == [Opcodes.NEW_TRANSACTION_STORES]
    assert out_db.batch_msg.opcode == Opcodes.NEW_TRANSACTION_STORES

    rows = out_db.batch_msg.rows
    assert len(rows) == 1
    r = rows[0]
    assert r.transaction_id == "tx-10"
    assert r.store_id == "st-1"
    assert r.store_name == "Main St"
    assert r.city == "Springfield"
    assert r.final_amount == "10.00"
    assert r.created_at.startswith("2025-02-01")
