from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ----------------------------
# Modelos de datos (mínimos)
# ----------------------------


@dataclass
class Metadata:
    queries: list[int]
    total_filter_steps: int
    filter_steps_mask: int = 0
    copy_info: Optional[Dict[int, int]] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataBatch:
    payload: Any
    metadata: Metadata


# ----------------------------
# Utilidades de bitmask
# ----------------------------


def is_bit_set(mask: int, idx: int) -> bool:
    return (mask >> idx) & 1 == 1


def set_bit(mask: int, idx: int) -> int:
    return mask | (1 << idx)


def first_zero_bit(mask: int, total_bits: int) -> Optional[int]:
    """Devuelve el índice del primer bit en 0 dentro de [0, total_bits)."""
    for i in range(total_bits):
        if not is_bit_set(mask, i):
            return i
    return None  # todos aplicados


# ----------------------------
# Interfaces de IO (abstracciones)
# ----------------------------


class BusProducer:
    def send_to_filters_pool(self, batch: DataBatch) -> None:
        # TODO: Publicar en un tópico/cola compartida por los workers de filtros.
        raise NotImplementedError

    def send_to_aggregator(self, batch: DataBatch) -> None:
        # TODO: Publicar en el tópico/cola del agregador.
        raise NotImplementedError


# ----------------------------
# Políticas por query (solo cantidad de steps y duplicación)
# ----------------------------


class QueryPolicyResolver:
    def steps_remaining(self, queries: list[int], steps_mask: int) -> bool:
        # TODO: De acuerdo a las queries y al steps_mask, devuelve True si faltan steps de filtrado
        #      o False si ya se completó el filtrado
        raise NotImplementedError

    def get_duplication_count(
        self, queries: list[int], next_step_index: int, batch: DataBatch
    ) -> int:
        # TODO: Política de duplicación *genérica* por query y/o por estado.
        #       Retornar 1 si no hay duplicación; N>1 si hay que duplicar N veces.
        return 1


# ----------------------------
# Router
# ----------------------------


class FilterRouter:
    def __init__(self, producer: BusProducer, policy: QueryPolicyResolver):
        self._producer = producer
        self._policy = policy
        self._log = logging.getLogger("filter-router")

    def process_batch(self, batch: DataBatch) -> None:
        meta = batch.metadata

        if meta.total_filter_steps is None:
            meta.total_filter_steps = self._policy.get_total_steps(meta.query_id)

        # 1) ¿Quedan steps? (sin conocer cuáles)
        next_step = first_zero_bit(meta.filter_steps_mask, meta.total_filter_steps)
        if self._policy.steps_remaining(meta.queries, meta.filter_steps_mask):
            # 2) Terminó el pipeline de filtros → al agregador
            self._log.debug("All steps done. → aggregator (query=%s)", meta.query_id)
            self._producer.send_to_aggregator(batch)
            return

        # 3) Marcar el próximo step (bit) ANTES de enviar a filtros
        meta.filter_steps_mask = set_bit(meta.filter_steps_mask, next_step)

        # 4) ¿Duplicamos? (sin semántica de step; solo cantidad)
        copies = max(
            1, int(self._policy.get_duplication_count(meta.query_id, next_step, batch))
        )

        if copies == 1:
            self._log.debug(
                "→ filters_pool step=%d (no duplication) query=%s",
                next_step,
                meta.query_id,
            )
            self._producer.send_to_filters_pool(next_step, batch)
            return

        # 5) Duplicación genérica: copiar batch y anotar copy_info
        self._log.debug(
            "Duplicating into %d copies. step=%d query=%s",
            copies,
            next_step,
            meta.query_id,
        )

        for i in range(copies):
            # TODO (performance): si payload es grande e inmutable, reemplazar deepcopy
            # por un esquema zero-copy (payload compartido; clonar solo metadata).
            b = copy.deepcopy(batch)
            b.metadata.copy_info = {"total": copies, "index": i}
            # Nota: no cambiamos next_step_index; la semántica la resuelve el worker.
            self._producer.send_to_filters_pool(next_step, b)
