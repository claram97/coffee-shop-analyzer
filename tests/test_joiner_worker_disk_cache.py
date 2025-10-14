# tests/test_joiner_q4_spool.py
import uuid

from joiner.worker import JoinerWorker
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawMenuItems, RawStore, RawTransaction
from protocol.messages import EOFMessage, NewMenuItems, NewStores, NewTransactions


def _db_bytes(
    inner_msg, *, table_ids, query_ids, client_id, batch_number=1, shards_info=None
):
    if shards_info is None:
        shards_info = [(1, 0)]
    # arma el DataBatch con todo lo necesario y lo serializa
    db = DataBatch(
        table_ids=table_ids,
        query_ids=query_ids,
        shards_info=shards_info,
        client_id=client_id,
    )
    db.batch_bytes = inner_msg.to_bytes()
    # opcional pero útil para logs/asserts
    db.batch_msg = inner_msg
    db.batch_number = batch_number
    return db.to_bytes()


def _eof_bytes(table_type: str, client_id: str) -> bytes:
    eof = EOFMessage()
    eof.create_eof_message(batch_number=0, table_type=table_type, client_id=client_id)
    return eof.to_bytes()


def test_q4_stash_persists_txstore_chunks(tmp_data_dir, fake_out, empty_inputs):
    """
    Verifica que el JoinerWorker guarde en FastSpool los (template_raw, lst)
    por usuario cuando procesa transactions Q4, usando la key {client_id}:{user_id}.
    """
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
        router_replicas=1,
    )

    # Usamos un client_id fijo para que todas las piezas calcen
    client_id = str(uuid.uuid4())

    # 1) Cache de menu_items (no lo usamos en Q4, pero es simétrico al flujo real)
    m = NewMenuItems()
    m.rows = [
        RawMenuItems(
            product_id="A",
            name="Americano",
            category="coffee",
            price="2.50",
            is_seasonal="0",
            available_from="2025-01-01",
            available_to="",
        )
    ]
    worker._on_raw_menu(
        _db_bytes(
            m, table_ids=[Opcodes.NEW_MENU_ITEMS], query_ids=[], client_id=client_id
        )
    )
    worker._on_raw_menu(
        _eof_bytes("menu_items", client_id)
    )  # marca phase ready para tablas que dependen

    # 2) Cache de stores (sí lo usamos para Q3/Q4 join)
    s = NewStores()
    s.rows = [
        RawStore(
            store_id="S1",
            store_name="1st Ave",
            street="1st",
            postal_code="1000",
            city="Metropolis",
            state="MT",
            latitude="0",
            longitude="0",
        )
    ]
    worker._on_raw_stores(
        _db_bytes(s, table_ids=[Opcodes.NEW_STORES], query_ids=[], client_id=client_id)
    )
    worker._on_raw_stores(_eof_bytes("stores", client_id))  # phase ready para TX

    # 3) Envía un batch de transactions con Q4 (user_id con pocas compras)
    tx = NewTransactions()
    tx.rows = [
        RawTransaction(
            transaction_id="T-001",
            store_id="S1",
            payment_method_id="pm-1",
            original_amount="10.00",
            discount_applied="0.00",
            final_amount="12.34",
            created_at="2025-01-15T10:00:00Z",
            user_id="U1",
            voucher_id="v-1",
        )
    ]
    raw_tx = _db_bytes(
        tx,
        table_ids=[Opcodes.NEW_TRANSACTION],
        query_ids=[4],  # Q4
        client_id=client_id,
        batch_number=7,
    )
    worker._on_raw_tx(raw_tx)

    # 4) Verifica que el FastSpool tenga la key {client}:{uid} y el contenido esperado
    key_prefix = f"{client_id}:"
    keys = worker._store.keys_with_prefix("q4_by_user")
    # puede haber más keys de otros tests, filtramos por cliente
    keys = [k for k in keys if k.startswith(key_prefix)]
    assert (
        f"{client_id}:U1" in keys
    ), f"no se encontró la key esperada en el spool: {keys}"

    items = worker._store.pop_all("q4_by_user", f"{client_id}:U1")
    assert len(items) == 1, f"esperaba un solo chunk por usuario; got={len(items)}"

    template_raw, lst = items[0]
    # lst debe ser la lista de RawTransactionStore que salió del join TX×Store
    assert isinstance(lst, list) and len(lst) == 1
    joined = lst[0]
    # checks fuertes sobre el join
    assert joined.transaction_id == "T-001"
    assert joined.store_id == "S1"
    assert joined.store_name == "1st Ave"
    assert joined.user_id == "U1"
    assert joined.final_amount == "12.34"
    assert joined.city == "Metropolis"
    assert joined.created_at == "2025-01-15T10:00:00Z"

    # template_raw debe ser un DataBatch serializado que sirve de plantilla
    out_db = DataBatch.deserialize_from_bytes(template_raw)
    # sin filas (metadata_only), pero conserva batch_number/meta/batch_status del original
    assert getattr(out_db.batch_msg, "rows", []) == []
    assert out_db.batch_number == 7
    # y dejó copiado el meta con info de copies (total/index)
    assert isinstance(out_db.meta, dict) and len(out_db.meta) >= 1
