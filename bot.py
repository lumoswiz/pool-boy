import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Set, Tuple

import click
from ape import Contract, accounts, chain
from ape.types import LogFilter
from silverback import SilverbackBot
from web3 import Web3

bot = SilverbackBot()


# Environment variables
def _required_env(key: str) -> str:
    val = os.getenv(key)
    if val is None or not val.strip():
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


def _require_checksum_addr(val: str, key: str) -> str:
    try:
        return Web3.to_checksum_address(val)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid address for {key}: {val!r}")


def _load_reserves_from_env(prefix: str = "RESERVE_") -> dict[str, str]:
    reserves: dict[str, str] = {}
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        sym = k[len(prefix) :]
        if not sym:
            continue
        addr = _require_checksum_addr(v, k)
        reserves[sym] = addr

    if not reserves:
        raise RuntimeError("No reserves configured. Set at least one RESERVE_<SYMBOL> env var, ")
    return reserves


POOL_ADDRESS = _require_checksum_addr(_required_env("POOL_ADDRESS"), "POOL_ADDRESS")
POOL = Contract(POOL_ADDRESS)
RESERVES = _load_reserves_from_env()
ADDR_TO_SYMBOL = {addr: sym for sym, addr in RESERVES.items()}


def addr(symbol: str) -> str | None:
    return RESERVES.get(symbol)


# Bot State
def _env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    if val is None or not val.strip():
        return default
    try:
        return int(val)
    except ValueError:
        return default


@dataclass
class BotState:
    allowed_reserves: set[str] = field(default_factory=set)
    chunk_blocks: int = _env_int("CHUNK_BLOCKS", 100)
    scan_interval_blocks: int = _env_int("SCAN_INTERVAL_BLOCKS", 5)
    rpc_max_log_range: int = _env_int("RPC_MAX_LOG_RANGE", 10)
    last_scan_block: int = 0
    next_end: int | None = None
    chunk_no: int = 0
    seen_debtors: set[str] = field(default_factory=set)
    backlog: set[str] = field(default_factory=set)
    backfill_busy: bool = False
    lock: Lock = field(default_factory=Lock)


# Backfill
def _iter_block_ranges(start_block: int, stop_block: int, step: int):
    cur = start_block
    while cur <= stop_block:
        end = min(cur + step - 1, stop_block)
        yield cur, end
        cur = end + 1


def _iter_logs_range(addresses, events, start_block: int, stop_block: int, step: int):
    if stop_block < start_block:
        return
    for start, end in _iter_block_ranges(start_block, stop_block, step):
        f = LogFilter(addresses=addresses, events=events, start_block=start, stop_block=end)
        try:
            for log in accounts.provider.get_contract_logs(f):
                yield log
        except Exception as ex:
            click.echo(f"[get_logs] {start}-{end} failed: {ex}")
            continue


def _iter_borrow_logs_range(start_block: int, stop_block: int, step: int):
    return _iter_logs_range([POOL.address], [POOL.Borrow.abi], start_block, stop_block, step)


def _maybe_debtor(log, allowed: Set[str]) -> str | None:
    return log.onBehalfOf if getattr(log, "reserve", None) in allowed else None


def _iter_borrowers_range(start_block: int, stop_block: int, step: int, allowed: set[str]):
    for log in _iter_borrow_logs_range(start_block, stop_block, step):
        d = _maybe_debtor(log, allowed)
        if d:
            yield d


@contextmanager
def _try_lock(lock: Lock, timeout: float = 0.2):
    got = lock.acquire(timeout=timeout)
    try:
        yield got
    finally:
        if got:
            lock.release()


def _plan_chunk(s: BotState, head_block: int) -> Tuple[int, int] | None:
    if s.next_end is None or s.next_end > head_block:
        s.next_end = head_block
    start = max(1, s.next_end - s.chunk_blocks + 1)
    stop = s.next_end
    return None if stop < start else (start, stop)


def _enqueue_many(addrs: Set[str], s: BotState) -> int:
    new = addrs - s.seen_debtors - s.backlog
    if not new:
        return 0
    s.seen_debtors |= new
    s.backlog |= new
    return len(new)


def _backfill_once(s: BotState, head_block: int):
    if s is None:
        return {"skipped_no_state": 1}

    with _try_lock(s.lock, timeout=0.2) as got:
        if not got:
            return {"skipped_locked": 1}

        plan = _plan_chunk(s, head_block)
        if plan is None:
            s.last_scan_block = head_block
            return {
                "noop": 1,
                "chunk_no": s.chunk_no,
                "chunk_start": max(1, (s.next_end or 1) - s.chunk_blocks + 1),
                "chunk_stop": (s.next_end or 0),
                "backlog_size": len(s.backlog),
                "seen_debtors_total": len(s.seen_debtors),
                "last_scan_block": s.last_scan_block,
            }

        start, stop = plan

        chunk_debtors = set(
            _iter_borrowers_range(start, stop, s.rpc_max_log_range, s.allowed_reserves)
        )
        added = _enqueue_many(chunk_debtors, s)

        s.next_end = start - 1
        s.chunk_no += 1
        s.last_scan_block = head_block

        return {
            "chunk_no": s.chunk_no,
            "chunk_start": start,
            "chunk_stop": stop,
            "chunk_debtors": len(chunk_debtors),
            "new_debtors": added,
            "backlog_size": len(s.backlog),
            "seen_debtors_total": len(s.seen_debtors),
            "last_scan_block": s.last_scan_block,
        }


def _process_live_borrow(s: BotState, log):
    debtor = _maybe_debtor(log, s.allowed_reserves)
    if not debtor:
        return {"borrow_event_ignored": 1}
    with _try_lock(s.lock, 0.2) as got:
        if not got:
            return {"borrow_event_skipped_locked": 1}
        added = _enqueue_many({debtor}, s)
        return {
            "borrow_event": 1,
            "added_to_backlog": added,
            "backlog_size": len(s.backlog),
            "seen_debtors_total": len(s.seen_debtors),
        }


# Silverback
@bot.on_startup()
def init_state(startup_state):
    s = BotState(allowed_reserves=set(RESERVES.values()))
    last = (
        startup_state.get("last_block_processed")
        if isinstance(startup_state, dict)
        else getattr(startup_state, "last_block_processed", None)
    )
    s.next_end = last or chain.blocks.head.number
    bot.state.data = s


@bot.on_(chain.blocks)
def handle_blocks(b):
    s = bot.state.data
    if b.number - s.last_scan_block < s.scan_interval_blocks:
        return
    return _backfill_once(s, b.number)


@bot.on_(POOL.Borrow, filter_args={"reserve": list(RESERVES.values())})
def on_borrow(log):
    return _process_live_borrow(bot.state.data, log)


@bot.on_shutdown()
def handle_on_shutdown():
    s = bot.state.data
    return {
        "backlog_size": len(s.backlog),
        "seen_debtors_total": len(s.seen_debtors),
    }
