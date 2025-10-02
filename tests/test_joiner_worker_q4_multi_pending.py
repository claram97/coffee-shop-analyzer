import types

import pytest

from joiner.worker import Q4, JoinerWorker
from protocol.constants import Opcodes
from protocol.entities import RawStore, RawTransaction, RawTransactionStoreUser, RawUser


# --- Helpers de "DataBatch" muy livianos para el test -------------------------
class _Msg:
    def __init__(self, opcode, rows):
        self.opcode = opcode
        self.rows = rows

    def to_bytes(self):
        # no se usa realmente (mockeamos _send); el worker lo puede setear
        return b"MSG"


class _DB:
    """Mock liviano de DataBatch con la interfaz usada por JoinerWorker."""

    def __init__(self, table_ids, query_ids, rows, meta=None, token=b""):
        self.table_ids = list(table_ids)
        self.query_ids = list(query_ids)
        self.meta = {} if meta is None else dict(meta)  # dict<u8,u8> en real
        self.batch_msg = _Msg(table_ids[0], list(rows))
        self.batch_bytes = None
        # token que devolverá to_bytes(); luego lo usaremos para mapear de vuelta
        self._token = token

    def to_bytes(self):
        return self._token


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
    )
    emits = []
    monkeypatch.setattr(worker, "_send", lambda db: emits.append(db))

    # 2) Cachear STORES para poder joinear TX->Store
    stores_db = _DB(
        table_ids=[Opcodes.NEW_STORES],
        query_ids=[],
        rows=[
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
        ],
    )
    monkeypatch.setattr(worker, "_decode_msg", lambda raw: ("db", stores_db))
    worker._on_raw_stores(b"STORES")

    # 3) Armar dos TX con Q4 para user 'u1' (distintas transactions)
    tx_a = RawTransaction(
        transaction_id="txA",
        store_id="s1",
        payment_method_id="",
        user_id="u1",
        original_amount="",
        discount_applied="",
        final_amount="10",
        created_at="tA",
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
    )

    # Cada _DB tiene un token distinto (simula los bytes del template_raw)
    db_tx_a = _DB([Opcodes.NEW_TRANSACTION], [Q4], [tx_a], token=b"TEMPLATE_A")
    db_tx_b = _DB([Opcodes.NEW_TRANSACTION], [Q4], [tx_b], token=b"TEMPLATE_B")

    # _decode_msg devolverá cada uno según el "raw" que pasemos
    def _decode_for_tx(raw):
        return ("db", db_tx_a if raw == b"TXA" else db_tx_b)

    monkeypatch.setattr(worker, "_decode_msg", _decode_for_tx)

    # En Q4, _on_raw_tx no emite nada; guarda particiones por usuario en el shelve
    worker._on_raw_tx(b"TXA")
    worker._on_raw_tx(b"TXB")
    assert emits == []  # aún nada emitido

    # 4) Simular DataBatch.deserialize_from_bytes(template_raw)
    # Cuando el worker levante el 'template_raw' (b"TEMPLATE_A" / b"TEMPLATE_B"),
    # le devolvemos un DB base (se va a sobreescribir table_ids y batch_msg).
    def _db_deserialize(body):
        if body == b"TEMPLATE_A":
            return _DB([Opcodes.NEW_TRANSACTION], [Q4], [])
        if body == b"TEMPLATE_B":
            return _DB([Opcodes.NEW_TRANSACTION], [Q4], [])
        pytest.fail("template_raw inesperado")

    import protocol.databatch as databatch_mod

    monkeypatch.setattr(
        databatch_mod.DataBatch, "deserialize_from_bytes", staticmethod(_db_deserialize)
    )

    # 5) Llegan USERS con 'u1' y Q4 → debe emitir dos resultados (uno por cada pending)
    users_db = _DB(
        [Opcodes.NEW_USERS],
        [Q4],
        [RawUser(user_id="u1", gender="", birthdate="1980-01-01", registered_at="")],
    )
    monkeypatch.setattr(worker, "_decode_msg", lambda raw: ("db", users_db))
    worker._on_raw_users(b"USERS")

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
