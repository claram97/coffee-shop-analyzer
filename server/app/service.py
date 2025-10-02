from common import utils

from .protocol import RawBet


def _to_utils_bet(rb: RawBet) -> utils.Bet:
    return utils.Bet(
        rb.agency, rb.first_name, rb.last_name, rb.document, rb.birthdate, rb.number
    )


def store_bets(raw_bets: list[RawBet]) -> int:
    """
    Converts transport-level RawBet objects to utils.Bet (domain model) and
    persists them via utils.store_bets.
    Returns the number of stored bets.
    """
    bets = [_to_utils_bet(rb) for rb in raw_bets]
    utils.store_bets(bets)
    return len(bets)


def compute_winners() -> dict[int, list[str]]:
    """Compute and group winners by agency.

    Loads all persisted bets, filters those that won (utils.has_won),
    and returns a dict mapping agency_id -> list of winner documents.
    """
    res: dict[int, list[str]] = {}
    for b in utils.load_bets():
        if utils.has_won(b):
            res.setdefault(b.agency, []).append(b.document)
    return res
