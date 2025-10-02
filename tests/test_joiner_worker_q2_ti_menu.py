import pytest

from joiner.worker import JoinerWorker
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawMenuItems, RawTransactionItem
from protocol.messages import NewMenuItems, NewTransactionItems


def _mk_menu_db():
    m = NewMenuItems()
    m.rows = [
        RawMenuItems(
            product_id="mi-1",
            name="Latte",
            category="coffee",
            price="3.50",
            is_seasonal="0",
            available_from="2025-01-01",
            available_to="",
        ),
        RawMenuItems(
            product_id="mi-2",
            name="Muffin",
            category="bakery",
            price="2.20",
            is_seasonal="0",
            available_from="2025-01-01",
            available_to="",
        ),
    ]
    from tests.conftest import _db_with

    return _db_with(m, table_ids=[Opcodes.NEW_MENU_ITEMS])


def _mk_ti_db_q2():
    ti = NewTransactionItems()
    ti.rows = [
        RawTransactionItem(
            transaction_id="tx-1",
            item_id="mi-1",
            quantity="2",
            unit_price="3.50",
            subtotal="7.00",
            created_at="2025-01-10T12:00:00Z",
        ),
        RawTransactionItem(
            transaction_id="tx-1",
            item_id="mi-999",  # inexistente (debería ignorarse en join)
            quantity="1",
            unit_price="99.00",
            subtotal="99.00",
            created_at="2025-01-10T12:01:00Z",
        ),
    ]
    from tests.conftest import _db_with

    return _db_with(ti, table_ids=[Opcodes.NEW_TRANSACTION_ITEMS], query_ids=[2])


def test_q2_join_ti_with_menu(tmp_data_dir, fake_out, empty_inputs):
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
    )

    # 1) cachear menu
    worker._on_raw_menu(_mk_menu_db())

    # 2) mandar transaction_items con Q2
    worker._on_raw_ti(_mk_ti_db_q2())

    # 3) debe haber salido 1 solo batch (el ítem con mi-1 joinea; mi-999 se descarta)
    assert len(fake_out.sent) == 1

    out_db = DataBatch.deserialize_from_bytes(fake_out.sent[0])
    # El worker setea: table_ids = [NEW_MENU_ITEMS, NEW_TRANSACTION_ITEMS]
    assert out_db.table_ids == [Opcodes.NEW_MENU_ITEMS, Opcodes.NEW_TRANSACTION_ITEMS]
    # Y reemplaza el mensaje por NewTransactionItemsMenuItems
    assert out_db.batch_msg.opcode == Opcodes.NEW_TRANSACTION_ITEMS_MENU_ITEMS
    rows = out_db.batch_msg.rows
    assert len(rows) == 1
    r = rows[0]
    # campos del join
    assert r.transaction_id == "tx-1"
    assert r.item_name == "Latte"
    assert r.quantity == "2"
    assert r.subtotal == "7.00"
    assert r.created_at.startswith("2025-01-10")
