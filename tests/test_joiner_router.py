import pytest

from joiner.router import (
    ExchangePublisherPool,
    JoinerRouter,
    TableRouteCfg,
    _hash_to_shard,
    _shard_key_for_row,
    is_broadcast_table,
)
from protocol.constants import Opcodes


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


def make_cfg():
    """Config mínima de ejemplo."""
    return {
        Opcodes.NEW_TRANSACTION: TableRouteCfg(
            "ex.tx", agg_shards=2, joiner_shards=4, key_pattern="rk.tx.{shard}"
        ),
        Opcodes.NEW_TRANSACTION_ITEMS: TableRouteCfg(
            "ex.ti", agg_shards=1, joiner_shards=3, key_pattern="rk.ti.{shard}"
        ),
        Opcodes.NEW_MENU_ITEMS: TableRouteCfg(
            "ex.menu", agg_shards=1, joiner_shards=2, key_pattern="rk.menu.{shard}"
        ),
        Opcodes.NEW_STORES: TableRouteCfg(
            "ex.stores", agg_shards=1, joiner_shards=2, key_pattern="rk.stores.{shard}"
        ),
        Opcodes.NEW_USERS: TableRouteCfg(
            "ex.users", agg_shards=1, joiner_shards=1, key_pattern="rk.users.{shard}"
        ),
    }


# --------------------------
# Unit tests de shard key
# --------------------------


def test_shard_key_for_query4_userid():
    row = type("R", (), {"user_id": "u42"})()
    assert _shard_key_for_row(Opcodes.NEW_TRANSACTION, row, [4]) == "u42"


def test_shard_key_for_query2_itemid():
    row = type("R", (), {"item_id": "itm99"})()
    assert _shard_key_for_row(Opcodes.NEW_TRANSACTION_ITEMS, row, [2]) == "itm99"


def test_shard_key_for_query3_storeid():
    row = type("R", (), {"store_id": "st123"})()
    assert _shard_key_for_row(Opcodes.NEW_TRANSACTION, row, [3]) == "st123"


def test_shard_key_none_for_other_cases():
    row = type("R", (), {"store_id": "st1"})()
    assert _shard_key_for_row(Opcodes.NEW_TRANSACTION, row, [1]) is None


# --------------------------
# Unit test de broadcast
# --------------------------


def test_broadcast_table_detection():
    assert is_broadcast_table(Opcodes.NEW_MENU_ITEMS, [2]) is True
    assert is_broadcast_table(Opcodes.NEW_STORES, [3]) is True
    assert is_broadcast_table(Opcodes.NEW_STORES, [4]) is True
    assert is_broadcast_table(Opcodes.NEW_USERS, [4]) is False


# --------------------------
# Integration-like tests
# --------------------------


def test_publish_and_broadcast(fake_pool):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    cfgs = make_cfg()
    router = JoinerRouter(
        in_mw=None, publisher_pool=pool, route_cfg=cfgs, fr_replicas=1
    )

    # Publish explícito
    raw = b"hello"
    cfg = cfgs[Opcodes.NEW_TRANSACTION]
    router._publish(cfg, shard=1, raw=raw)
    rk = "rk.tx.1"
    assert pubs[("ex.tx", rk)].sent == [b"hello"]

    # Broadcast a todos los shards
    router._broadcast(cfg, b"bye", shards=2)
    assert pubs[("ex.tx", "rk.tx.0")].sent[-1] == b"bye"
    assert pubs[("ex.tx", "rk.tx.1")].sent[-1] == b"bye"


# --------------------------
# Hash test (determinismo)
# --------------------------


def test_hash_to_shard_deterministic():
    k1 = _hash_to_shard("user42", 4)
    k2 = _hash_to_shard("user42", 4)
    assert k1 == k2
    assert 0 <= k1 < 4
