from joiner.worker import JoinerWorker
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawMenuItems, RawStore
from protocol.messages import NewMenuItems, NewStores


def _mk_menu_db():
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
    from tests.conftest import _db_with

    return _db_with(m, table_ids=[Opcodes.NEW_MENU_ITEMS])


def _mk_stores_db():
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
    from tests.conftest import _db_with

    return _db_with(s, table_ids=[Opcodes.NEW_STORES])


def test_disk_kv_persists_caches(tmp_data_dir, fake_out, empty_inputs):
    worker = JoinerWorker(
        in_mw=empty_inputs,
        out_results_mw=fake_out,
        data_dir=tmp_data_dir,
        logger=None,
        shard_index=0,
    )

    # cache menu y stores
    worker._on_raw_menu(_mk_menu_db())
    worker._on_raw_stores(_mk_stores_db())

    # Leemos del DiskKV directamente (como haría otro proceso/instancia)
    menu_idx = worker._store.get("menu_items", "full")
    stores_idx = worker._store.get("stores", "full")

    assert isinstance(menu_idx, dict) and "A" in menu_idx
    assert menu_idx["A"].name == "Americano"

    assert isinstance(stores_idx, dict) and "S1" in stores_idx
    assert stores_idx["S1"].store_name == "1st Ave"
