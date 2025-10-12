import pytest

from joiner.router import (
    ExchangePublisherPool,
    JoinerRouter,
    build_route_cfg_from_config,
)
from protocol.constants import Opcodes
from protocol.messages import EOFMessage


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
    """
    Config dummy mínima para que build_route_cfg_from_config funcione.
    """

    class DummyNames:
        joiner_router_exchange_fmt = "jx.{table}"
        joiner_router_rk_fmt = "join.{table}.shard.{shard:02d}"

    class DummyWorkers:
        joiners = 3  # default para livianas si no hay joiner_shards específicos

    class DummyBroker:
        host = "in-mem"

    class DummyCfg:
        names = DummyNames()
        workers = DummyWorkers()
        broker = DummyBroker()

        # 3 particiones de aggregator por tabla
        def agg_partitions(self, table: str) -> int:
            return 3

        # 4 shards de joiner por tabla
        def joiner_partitions(self, table: str) -> int:
            return 4

    return DummyCfg()


def _eof_bytes_for(table_type: str) -> bytes:
    eof = EOFMessage().create_eof_message(batch_number=0, table_type=table_type)
    return eof.to_bytes()


def test_eof_message_round_trip_includes_client_id():
    client_id = "123e4567-e89b-12d3-a456-426614174000"
    eof = EOFMessage().create_eof_message(
        batch_number=42,
        table_type="transactions",
        client_id=client_id,
    )
    raw = eof.to_bytes()

    parsed = EOFMessage.deserialize_from_bytes(raw)

    assert parsed.get_table_type() == "transactions"
    assert parsed.batch_number == 42
    assert parsed.client_id == client_id


def test_table_eof_broadcast_transactions(fake_pool, cfg_obj):
    pool, pubs = fake_pool.pool, fake_pool.pubs
    route_cfg = build_route_cfg_from_config(cfg_obj)
    router = JoinerRouter(in_mw=None, publisher_pool=pool, route_cfg=route_cfg)

    # Sanity: aseguramos valores esperados del cfg
    agg_parts = route_cfg[Opcodes.NEW_TRANSACTION].agg_shards  # 3
    joiner_shards = route_cfg[Opcodes.NEW_TRANSACTION].joiner_shards  # 4
    assert agg_parts == 3
    assert joiner_shards == 4

    # EOF de "transactions" (string, como lo manda el Aggregator)
    raw_eof = _eof_bytes_for("transactions")

    # 1) Antes de completar los agg_shards, NO debe broadcastear
    router._on_raw(raw_eof)  # EOF #1
    router._on_raw(raw_eof)  # EOF #2

    pubs_with_data_mid = [(rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent]
    assert (
        len(pubs_with_data_mid) == 0
    ), "No debería haber broadcast con EOFs incompletos"

    # 2) Al llegar al EOF #3 (agg_shards), debe broadcastear a TODOS los joiner shards
    router._on_raw(raw_eof)  # EOF #3

    pubs_with_data = [(ex, rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent]
    # Deberíamos tener exactamente 'joiner_shards' publicaciones
    assert len(pubs_with_data) == joiner_shards

    # Y cada publicación debería contener exactamente 1 mensaje: el EOF serializado
    for ex, rk, sent in pubs_with_data:
        assert len(sent) == 1
        assert (
            sent[0] == raw_eof
        ), f"El payload de broadcast debe ser el EOF original. rk={rk}"

    # 3) Verificamos reset: con 1 EOF más no debería volver a broadcastear hasta completar otro ciclo de 3
    # limpiamos el buffer de publishers para observar sólo lo nuevo
    for _, _, sent in pubs_with_data:
        sent.clear()

    router._on_raw(raw_eof)  # EOF #1 del nuevo ciclo
    pubs_after_reset = [
        (ex, rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent
    ]
    assert (
        len(pubs_after_reset) == 0
    ), "Después del reset no debe broadcastear hasta completar el nuevo ciclo"

    # completamos el nuevo ciclo
    router._on_raw(raw_eof)  # EOF #2
    router._on_raw(raw_eof)  # EOF #3 -> ahora sí debe broadcastear
    pubs_after_full_cycle = [
        (ex, rk, pub.sent) for (ex, rk), pub in pubs.items() if pub.sent
    ]
    assert len(pubs_after_full_cycle) == joiner_shards
    for ex, rk, sent in pubs_after_full_cycle:
        assert len(sent) == 1 and sent[0] == raw_eof
