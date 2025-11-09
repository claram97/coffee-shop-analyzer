from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class BrokerCfg:
    host: str
    port: int
    username: str
    password: str
    vhost: str
    management_port: int


@dataclass(frozen=True)
class QueuesCfg:
    prefetch_count: int
    durable: bool


@dataclass(frozen=True)
class WorkersCfg:
    filters: int
    aggregators: int
    joiners: int
    results: int
    orchestrators: int


@dataclass(frozen=True)
class NamesCfg:
    filters_pool_queue: str
    filter_router_exchange_fmt: str
    filter_router_rk_fmt: str
    aggregator_queue_fmt: str
    aggregator_to_joiner_router_queue_fmt: str
    joiner_router_exchange_fmt: str
    joiner_router_rk_fmt: str
    results_controller_queue: str
    rc_to_orch_queue: str
    orchestrator_input_queue: str
    joiner_queue_fmt: str
    orch_to_fr_exchange: str
    orch_to_fr_rk_fmt: str
    orch_to_fr_queue_fmt: str


@dataclass(frozen=True)
class RoutersCfg:
    filter: int
    joiner: int
    results: int


class ConfigError(Exception):
    pass


class Config:
    """
    Carga y expone la configuración del sistema desde un INI.
    No imprime ni exporta variables de entorno.
    """

    def __init__(self, ini_path: str):
        self._path = os.path.abspath(ini_path)
        if not os.path.exists(self._path):
            raise ConfigError(f"INI file not found: {self._path}")

        cp = configparser.ConfigParser()
        cp.read(self._path)

        try:
            self.agg_shards: Dict[str, int] = {
                k.strip(): int(v) for k, v in cp["agg_shards"].items()
            }
        except KeyError:
            self.agg_shards = {}

        try:
            self.joiner_shards: Dict[str, int] = {
                k.strip(): int(v) for k, v in cp["joiner_shards"].items()
            }
        except KeyError:
            self.joiner_shards = {}

        b = cp["broker"]
        self.broker = BrokerCfg(
            host=b.get("host", "localhost"),
            port=b.getint("port", 5672),
            username=b.get("username", "guest"),
            password=b.get("password", "guest"),
            vhost=b.get("vhost", "/"),
            management_port=b.getint("management_port", 15672),
        )

        q = cp["queues"]
        self.queues = QueuesCfg(
            prefetch_count=q.getint("prefetch_count", 10),
            durable=q.getboolean("durable", True),
        )

        w_filters = cp.getint("filters", "workers", fallback=1)
        w_aggs = cp.getint("aggregators", "workers", fallback=1)
        w_joiners = cp.getint("joiners", "workers", fallback=1)
        w_results = cp.getint("results", "workers", fallback=1)
        w_orchestrators = cp.getint("orchestrator", "workers", fallback=1)
        self.workers = WorkersCfg(
            w_filters, w_aggs, w_joiners, w_results, w_orchestrators
        )

        n = cp["names"]
        self.names = NamesCfg(
            filters_pool_queue=n.get("filters_pool_queue"),
            filter_router_exchange_fmt=n.get("filter_router_exchange_fmt"),
            filter_router_rk_fmt=n.get("filter_router_rk_fmt"),
            aggregator_queue_fmt=n.get("aggregator_queue_fmt"),
            aggregator_to_joiner_router_queue_fmt=n.get(
                "aggregator_to_joiner_router_queue_fmt"
            ),
            joiner_router_exchange_fmt=n.get("joiner_router_exchange_fmt"),
            joiner_router_rk_fmt=n.get("joiner_router_rk_fmt"),
            results_controller_queue=n.get("results_controller_queue"),
            rc_to_orch_queue=n.get("rc_to_orch_queue"),
            orchestrator_input_queue=n.get("orchestrator_input_queue"),
            joiner_queue_fmt=n.get("joiner_queue_fmt"),
            orch_to_fr_exchange=n.get("orch_to_fr_exchange"),
            orch_to_fr_rk_fmt=n.get("orch_to_fr_rk_fmt"),
            orch_to_fr_queue_fmt=n.get("orch_to_fr_queue_fmt"),
        )

        r_filters = cp.getint("filters", "routers", fallback=1)
        r_joiners = cp.getint("joiners", "routers", fallback=1)
        r_results = cp.getint("results", "routers", fallback=1)
        self.routers = RoutersCfg(r_filters, r_joiners, r_results)

        # Joiner worker configuration
        self.joiner_write_buffer_size = cp.getint("joiners", "write_buffer_size", fallback=100)

    def joiner_partitions(self, table: str) -> int:
        """Shards de salida del Joiner Router para TABLE (consumen Joiners)."""
        t = str(table).strip()
        return int(self.joiner_shards.get(t, 1))

    def filter_router_exchange(self, table: str) -> str:
        return self.names.filter_router_exchange_fmt.format(table=table)

    def filter_router_rk(self, table: str, pid: int) -> str:
        return self.names.filter_router_rk_fmt.format(table=table, pid=int(pid))

    def aggregator_queue(self, table: str, pid: int) -> str:
        return self.names.aggregator_queue_fmt.format(table=table, pid=int(pid))

    def aggregator_to_joiner_router_queue(
        self, table: str, pid: int, replica: int
    ) -> str:
        return self.names.aggregator_to_joiner_router_queue_fmt.format(
            table=table, pid=int(pid), replica=int(replica)
        )

    def joiner_router_exchange(self, table: str) -> str:
        return self.names.joiner_router_exchange_fmt.format(table=table)

    def joiner_router_rk(self, table: str, shard: int) -> str:
        return self.names.joiner_router_rk_fmt.format(table=table, shard=int(shard))

    def joiner_queue(self, table: str, shard: int) -> str:
        return self.names.joiner_queue_fmt.format(table=table, shard=int(shard))

    def orchestrator_rk(self, pid: int) -> str:
        return self.names.orch_to_fr_rk_fmt.format(pid=int(pid))

    def filter_router_in_queue(self, pid: int) -> str:
        return self.names.orch_to_fr_queue_fmt.format(pid=int(pid))
