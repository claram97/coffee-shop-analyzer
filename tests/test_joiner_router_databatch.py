import pytest  # type: ignore[import-not-found]

from joiner.router import (
    ExchangePublisherPool,
    JoinerRouter,
    build_route_cfg_from_config,
)
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.messages import (
    NewMenuItems,
    NewStores,
    NewTransactionItems,
    NewTransactions,
)

TEST_CLIENT_ID = "00000000-0000-0000-0000-000000000002"


class FakePublisher:
    def __init__(self):
        self.sent = []

    def send(self, raw: bytes):
        self.sent.append(raw)


class PoolBundle:
    def __init__(self, pool, pubs):
        self.pool = pool
        self.pubs = pubs


@pytest.fixture
def fake_pool():
    pubs = {}

    def factory(exchange_name, rk):
        pub = FakePublisher()
        pubs[(exchange_name, rk)] = pub
        return pub

    return PoolBundle(ExchangePublisherPool(factory), pubs)


@pytest.fixture
def cfg_obj():
    """Config mínima simulada para route_cfg_from_config."""

    class DummyNames:
        joiner_router_exchange_fmt = "ex.{table}"
        joiner_router_rk_fmt = "rk.{table}.{shard}"

    class DummyCfg:
        names = DummyNames()
        workers = type("W", (), {"joiners": 2})()
        broker = type("B", (), {"host": "fake"})()

        def agg_partitions(self, table):
            return 2

        def joiner_partitions(self, table):
            return 2

    return DummyCfg()


# ------------------------
# Tests con DataBatch real
# ------------------------


def test_shard_transactions(fake_pool, cfg_obj):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    route_cfg = build_route_cfg_from_config(cfg_obj)
    router = JoinerRouter(None, pool, route_cfg)

    msg = NewTransactions()
    rows = [
        {
            "transaction_id": "tx1",
            "store_id": "s1",
            "final_amount": "10",
            "created_at": "2025-01-01",
        },
        {
            "transaction_id": "tx2",
            "store_id": "s2",
            "final_amount": "20",
            "created_at": "2025-01-02",
        },
    ]
    db = DataBatch(
        table_ids=[Opcodes.NEW_TRANSACTION],
        batch_bytes=msg.to_bytes(),
        query_ids=[3],
        client_id=TEST_CLIENT_ID,
    )
    db.batch_msg = msg
    db.batch_msg.rows = rows
    db.batch_bytes = db.batch_msg.to_bytes()
    raw = db.to_bytes()

    # simular llegada de batch
    router._on_raw(raw)

    # debería haberse particionado en 2 shards
    sent_rks = [rk for (_, rk), pub in pubs.items() if pub.sent]
    assert any(
        "s1" in str(pub.sent[0]) or "s2" in str(pub.sent[0]) for pub in pubs.values()
    )
    assert len(sent_rks) >= 2


def test_broadcast_stores(fake_pool, cfg_obj):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    route_cfg = build_route_cfg_from_config(cfg_obj)
    router = JoinerRouter(None, pool, route_cfg)

    msg = NewStores()
    rows = [{"store_id": "s1", "store_name": "store1", "city": "A"}]
    db = DataBatch(
        table_ids=[Opcodes.NEW_STORES],
        batch_bytes=msg.to_bytes(),
        query_ids=[3],
        client_id=TEST_CLIENT_ID,
    )
    db.batch_msg = msg

    db.batch_msg.rows = rows
    db.batch_bytes = db.batch_msg.to_bytes()
    raw = db.to_bytes()

    # con query 3, stores debería broadcastear a todos los shards
    router._on_raw(raw)

    pubs_with_data = [(rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent]
    assert len(pubs_with_data) == route_cfg[Opcodes.NEW_STORES].joiner_shards
    for rk, sent in pubs_with_data:
        assert b"store1" in sent[0]


def test_menu_items_broadcast_for_q2(fake_pool, cfg_obj):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    route_cfg = build_route_cfg_from_config(cfg_obj)
    router = JoinerRouter(None, pool, route_cfg)

    msg = NewMenuItems()
    rows = [{"item_id": "i1", "item_name": "burger"}]
    db = DataBatch(
        table_ids=[Opcodes.NEW_MENU_ITEMS],
        batch_bytes=msg.to_bytes(),
        query_ids=[2],
        client_id=TEST_CLIENT_ID,
    )
    db.batch_msg = msg
    db.batch_msg.rows = rows
    db.batch_bytes = db.batch_msg.to_bytes()
    raw = db.to_bytes()

    router._on_raw(raw)

    pubs_with_data = [(rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent]
    assert len(pubs_with_data) == route_cfg[Opcodes.NEW_MENU_ITEMS].joiner_shards
    for rk, sent in pubs_with_data:
        assert b"burger" in sent[0]


def test_transaction_items_shard_by_item(fake_pool, cfg_obj):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    route_cfg = build_route_cfg_from_config(cfg_obj)
    router = JoinerRouter(None, pool, route_cfg)

    msg = NewTransactionItems()
    rows = [
        {"transaction_id": "t1", "item_id": "itm1", "quantity": "1"},
        {"transaction_id": "t2", "item_id": "itm2", "quantity": "2"},
    ]
    db = DataBatch(
        table_ids=[Opcodes.NEW_TRANSACTION_ITEMS],
        batch_bytes=msg,
        query_ids=[2],
        client_id=TEST_CLIENT_ID,
    )
    db.batch_msg = msg
    db.batch_msg.rows = rows
    db.batch_bytes = db.batch_msg.to_bytes()
    raw = db.to_bytes()

    router._on_raw(raw)

    # debe haber caído en 2 shards distintos
    pubs_with_data = [(rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent]
    assert len(pubs_with_data) >= 2
    assert any(b"itm1" in sent[0] for _, sent in pubs_with_data)
