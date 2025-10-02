import pytest

from joiner.worker import JoinerWorker
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawStore, RawTransaction, RawUser
from protocol.messages import NewStores, NewTransactions, NewUsers


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


def _mk_tx_db_q4_multiusers():
    """Dos usuarios distintos en el mismo batch de TX"""
    tx = NewTransactions()
    tx.rows = [
        RawTransaction(
            transaction_id="tx-A",
            store_id="st-1",
            payment_method_id="pm-1",
            user_id="u-1",
            original_amount="10.00",
            discount_applied="0.00",
            final_amount="10.00",
            created_at="2025-02-01T10:00:00Z",
        ),
        RawTransaction(
            transaction_id="tx-B",
            store_id="st-1",
            payment_method_id="pm-1",
            user_id="u-2",
            original_amount="9.00",
            discount_applied="0.00",
            final_amount="9.00",
            created_at="2025-02-01T11:00:00Z",
        ),
    ]
    from tests.conftest import _db_with

    # Q4
    return _db_with(tx, table_ids=[Opcodes.NEW_TRANSACTION], query_ids=[4])


def _mk_users_db(u1_birth="1990-01-01", u2_birth="1985-05-05"):
    us = NewUsers()
    us.rows = [
        RawUser(
            user_id="u-1", gender="m", birthdate=u1_birth, registered_at="2020-01-01"
        ),
        RawUser(
            user_id="u-2", gender="f", birthdate=u2_birth, registered_at="2020-01-02"
        ),
    ]
    from tests.conftest import _db_with

    return _db_with(us, table_ids=[Opcodes.NEW_USERS], query_ids=[4])


def test_q4_partition_and_template_meta(tmp_data_dir, fake_out, empty_inputs):
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
    )

    # 1) cachear stores (para poder armar TX+Store)
    worker._on_raw_stores(_mk_stores_db())

    # 2) mandar TX con Q4 que tiene 2 usuarios distintos
    worker._on_raw_tx(_mk_tx_db_q4_multiusers())

    # En Q4, _on_raw_tx NO envía nada aún; guarda en disco por usuario
    assert len(fake_out.sent) == 0

    # 3) llegan los users con Q4 -> debe emitir dos batches NEW_TRANSACTION_STORES_USERS
    worker._on_raw_users(_mk_users_db())

    # ahora deberían existir 2 emisiones
    assert len(fake_out.sent) == 2

    # Verificamos que cada salida es NEW_TRANSACTION_STORES_USERS
    out1 = DataBatch.deserialize_from_bytes(fake_out.sent[0])
    out2 = DataBatch.deserialize_from_bytes(fake_out.sent[1])

    assert out1.table_ids == [Opcodes.NEW_TRANSACTION_STORES_USERS]
    assert out2.table_ids == [Opcodes.NEW_TRANSACTION_STORES_USERS]
    assert out1.batch_msg.opcode == Opcodes.NEW_TRANSACTION_STORES_USERS
    assert out2.batch_msg.opcode == Opcodes.NEW_TRANSACTION_STORES_USERS

    # Verificá contenido (u-1 y u-2)
    rows1 = out1.batch_msg.rows
    rows2 = out2.batch_msg.rows
    assert len(rows1) == 1 and len(rows2) == 1
    r1, r2 = rows1[0], rows2[0]
    got_users = {r1.user_id, r2.user_id}
    assert got_users == {"u-1", "u-2"}
    # birthdate debe venir del usuario; los demás campos vienen del join tx+store
    assert r1.store_name == "Main St" and r2.store_name == "Main St"
    assert r1.transaction_id in {"tx-A", "tx-B"}
    assert r2.transaction_id in {"tx-A", "tx-B"}

    # Chequeo del template_raw.meta (total_particiones -> numero_de_particion)
    # El worker usa un dict<u8,u8> (DataBatch.meta).
    # Para cada out_db, la meta DEBERÍA contener una única entrada (k=total, v=index)
    for out_db in (out1, out2):
        meta = getattr(out_db, "meta", {})
        # Debe haber exactamente 1 entrada si el worker setea (total -> idx)
        assert len(meta) == 1, f"meta inesperada: {meta}"
        [(total, idx)] = list(meta.items())
        assert total == 2  # total particiones (2 usuarios)
        assert idx in (0, 1)  # índice de copia para ese user
