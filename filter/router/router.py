from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, NamedTuple, Optional

# ----------------------------
# CopyInfo: historial ordenado de fan-outs anidados
# ----------------------------


class CopyInfo(NamedTuple):
    index: int  # índice de esta copia en este fan-out
    total: int  # total de copias generadas en este fan-out


# ----------------------------
# Modelos
# ----------------------------


@dataclass
class Metadata:
    table_name: str
    queries: List[int]  # p.ej. [1, 3, 4]
    total_filter_steps: int  # cantidad total de steps posibles para ESTA rama
    filter_steps_mask: int = 0  # bit=1 => step ya aplicado
    copy_info: List[CopyInfo] = field(default_factory=list)


@dataclass
class DataBatch:
    payload: Any
    metadata: Metadata


# ----------------------------
# Bitmask utils
# ----------------------------


def is_bit_set(mask: int, idx: int) -> bool:
    return ((mask >> idx) & 1) == 1


def set_bit(mask: int, idx: int) -> int:
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None


# ----------------------------
# IO
# ----------------------------


class BusProducer:
    def send_to_filters_pool(self, batch: DataBatch, step: int) -> None:
        """Publicar en la cola/tópico de filtros; incluir 'step' en headers."""
        raise NotImplementedError

    def send_to_aggregator(self, batch: DataBatch) -> None:
        """Publicar en la cola/tópico del agregador."""
        raise NotImplementedError

    def requeue_to_router(self, batch: DataBatch) -> None:
        """Volver a publicar en la cola del router (evita recursión)."""
        raise NotImplementedError


# ----------------------------
# La policy es la tuya (ya implementada)
# ----------------------------


class QueryPolicyResolver:
    def steps_remaining(
        self, batch_table_name: str, batch_queries: list[int], steps_done: int
    ) -> bool:
        if batch_table_name == "transactions":
            if len(batch_queries) == 3:
                if steps_done == 0:
                    return True
                return False
            if len(batch_queries) == 1 and batch_queries[0] == 4:
                return False
            if len(batch_queries) == 2:
                if steps_done == 1:
                    return True
                return False
            if len(batch_queries) == 1 and batch_queries[0] == 3:
                return False
            if len(batch_queries) == 1 and batch_queries[0] == 1:
                if steps_done == 2:
                    return True
                return False

        if batch_table_name == "users":
            if steps_done == 0:
                return True
            return False

        if batch_table_name == "transaction_items":
            if steps_done == 0:
                return True
            return False

        return False

    # Transactions:
    # 1. Enviar a filter
    # 2. Duplicar batch -> c1, c2
    # 3. Dejar q4 en queries de c1
    # 4. Enviar c1 a aggregator
    # 5. Dejar q1 y q3 en queries de c2
    # 6. Enviar c2 a Filter
    # 7. Duplicar c2 -> c21, c22
    # 8. Dejar q3 en c21
    # 9. Enviar c21 a aggregator
    # 10. Dejar q1 en c22
    # 10. Enviar c22 a filter
    # 11. Enviar c22 a aggregator

    # Users:
    # 1. Enviar a filter
    # 2. Enviar a aggregator

    # Transaction_items:
    # 1. Enviar a filter
    # 2. Enviar a aggregator
    def get_duplication_count(
        self,
        batch_queries: list[int],
    ) -> int:
        if len(batch_queries) > 1:
            return 2

    def get_new_batch_queries(
        self, batch_table_name: str, batch_queries: list[int], copy_number: int
    ) -> list[int]:
        if batch_table_name == "transactions":
            if len(batch_queries) == 3:
                if copy_number == 1:
                    return [4]
                return [1, 3]
            if len(batch_queries) == 2:
                if copy_number == 1:
                    return [3]
                return [1]


# ----------------------------
# Router
# ----------------------------


class FilterRouter:
    def __init__(self, producer: BusProducer, policy: QueryPolicyResolver):
        self._p = producer
        self._pol = policy
        self._log = logging.getLogger("filter-router")

    def process_batch(self, batch: DataBatch) -> None:
        m = batch.metadata

        # 1) Calcular próximo step pendiente
        next_step = first_zero_bit(m.filter_steps_mask, m.total_filter_steps)

        # 2) Si la policy dice que aún quedan steps → marcar y enviar a filtros
        if next_step is not None and self._pol.steps_remaining(
            m.table_name, m.queries, steps_done=next_step
        ):
            # “Antes de mandar el batch a filtrar, pone en 1 el primer bit…”
            m.filter_steps_mask = set_bit(m.filter_steps_mask, next_step)
            self._log.debug(
                "→ filters step=%d table=%s queries=%s mask=%s",
                next_step,
                m.table_name,
                m.queries,
                bin(m.filter_steps_mask),
            )
            self._p.send_to_filters_pool(batch, step=next_step)
            return

        # 3) No quedan steps (o la policy indica que no corresponde filtrar ahora):
        #    decidir duplicación.
        dup_count = self._pol.get_duplication_count(m.queries)
        dup_count = int(dup_count) if dup_count is not None else 1

        if dup_count <= 1:
            # 4) Sin fan-out → al agregador
            self._log.debug(
                "Steps done / no-dup. → aggregator table=%s queries=%s",
                m.table_name,
                m.queries,
            )
            self._p.send_to_aggregator(batch)
            return

        # 5) Fan-out: crear N copias. Cada copia ajusta queries.
        self._log.debug(
            "Fan-out x%d table=%s queries=%s", dup_count, m.table_name, m.queries
        )

        for i in range(dup_count):
            new_queries = self._pol.get_new_batch_queries(
                m.table_name, m.queries, copy_number=i
            )
            if not new_queries:
                # Defensa: si la policy devolviera vacío, degradamos a “misma rama”
                new_queries = list(m.queries)

            b = copy.deepcopy(batch)
            b.metadata.copy_info = m.copy_info + [CopyInfo(index=i, total=dup_count)]
            b.metadata.queries = new_queries

            # Reencolar la copia para que vuelva a pasar por la misma FSM del router
            self._p.requeue_to_router(b)
