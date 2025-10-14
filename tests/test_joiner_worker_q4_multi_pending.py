import types

import pytest
from conftest import TEST_CLIENT_ID, _db_with

from joiner.worker import Q4, JoinerWorker
from protocol.constants import Opcodes
from protocol.entities import RawStore, RawTransaction, RawTransactionStoreUser, RawUser
from protocol.messages import NewStores, NewTransactions, NewUsers


# --- Fixtures -----------------------------------------------------------------
@pytest.fixture
def tmp_data_dir(tmp_path):
    # carpeta por test para el shelve
    d = tmp_path / "joiner-data"
    d.mkdir()
    return str(d)


@pytest.fixture
def fake_out():
    class FakeOut:
        def __init__(self):
            self.sent = []

        def send(self, raw):
            self.sent.append(raw)

    return FakeOut()


@pytest.fixture
def empty_inputs():
    # no usamos consumidores porque llamamos directamente a los handlers
    return {}


# --- Test ---------------------------------------------------------------------
def test_q4_two_pending_batches_same_user(
    tmp_data_dir, fake_out, empty_inputs, monkeypatch
):
    """
    Dos batches Q4 (TX+Store) para user 'u1' quedan pendientes en disco.
    Cuando llega Users con 'u1', el worker debe emitir DOS resultados distintos
    con NEW_TRANSACTION_STORES_USERS.
    """

    # 1) Instanciar worker con un _send capturado (evita serializar a bytes)
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,  # no lo usaremos: vamos a monkeypatchear _send
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
        router_replicas=1,
    )
    emits = []
    monkeypatch.setattr(worker, "_send", lambda db: emits.append(db))

    inner_msg = NewStores()
    inner_msg.rows = [
        RawStore(
            store_id="s1",
            store_name="S-One",
            street="",
            postal_code="",
            city="C",
            state="",
            latitude="",
            longitude="",
        )
    ]

    # 2) Cachear STORES para poder joinear TX->Store
    stores_db = _db_with(
        inner_msg,
        table_ids=[Opcodes.NEW_STORES],
        query_ids=[],
    )
    worker._on_raw_stores(stores_db)

    # 3) Enablear fase de transactions
    worker._on_table_eof(Opcodes.NEW_STORES, TEST_CLIENT_ID)
    worker._on_table_eof(Opcodes.NEW_MENU_ITEMS, TEST_CLIENT_ID)

    # 4) Armar dos TX con Q4 para user 'u1' (distintas transactions)
    tx_a = RawTransaction(
        transaction_id="txA",
        store_id="s1",
        payment_method_id="",
        user_id="u1",
        original_amount="",
        discount_applied="",
        final_amount="10",
        created_at="tA",
        voucher_id="v-1",
    )
    tx_b = RawTransaction(
        transaction_id="txB",
        store_id="s1",
        payment_method_id="",
        user_id="u1",
        original_amount="",
        discount_applied="",
        final_amount="20",
        created_at="tB",
        voucher_id="v-2",
    )
    inner_msg_a = NewTransactions()
    inner_msg_a.rows = [tx_a]
    inner_msg_b = NewTransactions()
    inner_msg_b.rows = [tx_b]

    db_tx_a = _db_with(
        inner_msg_a,
        table_ids=[Opcodes.NEW_TRANSACTION],
        query_ids=[Q4],
    )
    db_tx_b = _db_with(
        inner_msg_b,
        table_ids=[Opcodes.NEW_TRANSACTION],
        query_ids=[Q4],
    )

    worker._on_raw_tx(db_tx_a)
    worker._on_raw_tx(db_tx_b)
    assert emits == []

    # 5) Enablear fase de Users
    worker._on_table_eof(Opcodes.NEW_TRANSACTION, TEST_CLIENT_ID)
    worker._on_table_eof(Opcodes.NEW_TRANSACTION_ITEMS, TEST_CLIENT_ID)

    # 6) Llegan USERS con 'u1' y Q4 → debe emitir dos resultados (uno por cada pending)
    inner_msg_u = NewUsers()
    inner_msg_u.rows = [
        RawUser(user_id="u1", gender="", birthdate="1980-01-01", registered_at="")
    ]
    db_u = _db_with(
        inner_msg_u,
        table_ids=[Opcodes.NEW_USERS],
        query_ids=[Q4],
    )
    worker._on_raw_users(db_u)

    # Deben haber dos emisiones
    assert len(emits) == 2

    # Verificar que ambos sean NEW_TRANSACTION_STORES_USERS y pertenecen a u1
    tx_ids = set()
    for out_db in emits:
        assert out_db.table_ids == [Opcodes.NEW_TRANSACTION_STORES_USERS]
        rows = out_db.batch_msg.rows
        assert rows, "debe tener filas"
        assert all(isinstance(r, RawTransactionStoreUser) for r in rows)
        assert all(r.user_id == "u1" for r in rows)
        tx_ids.update(r.transaction_id for r in rows)

    # Deben provenir de las dos transacciones pendientes
    assert tx_ids == {"txA", "txB"}
