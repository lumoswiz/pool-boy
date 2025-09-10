import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Set

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
    seen_borrow_block: dict[str, int] = field(default_factory=dict)
    backlog: set[str] = field(default_factory=set)
    backfill_busy: bool = False
    lock: Lock = field(default_factory=Lock)
    last_backfill_head: int = 0


def _cap_to_head(s: BotState, head: int) -> None:
    if s.next_end is not None:
        s.next_end = min(s.next_end, head)
    if s.last_scan_block is not None:
        s.last_scan_block = min(s.last_scan_block, head)


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


def _enqueue_many_newer(addrs: Set[str], height: int, s: BotState) -> int:
    added = 0
    for a in addrs:
        prev = s.seen_borrow_block.get(a, -1)
        if prev < height:
            s.seen_borrow_block[a] = height
            if a not in s.backlog:
                s.backlog.add(a)
                added += 1
    return added


FORWARD = "forward"
BACKFILL = "backfill"


def _plan_window(s: BotState, head: int, mode: str) -> tuple[int, int] | None:
    if mode == FORWARD:
        start = (s.last_scan_block or 0) + 1
        if start > head:
            return None
        stop = min(start + s.chunk_blocks - 1, head)
        return start, stop
    if mode == BACKFILL:
        if s.next_end is None or s.next_end > head:
            s.next_end = head
        stop = s.next_end
        if stop < 1:
            return None
        start = max(1, stop - s.chunk_blocks + 1)
        if stop < start:
            return None
        return start, stop
    raise ValueError(f"unknown mode: {mode}")


def _scan_window(s: BotState, start: int, stop: int) -> tuple[int, int]:
    addrs = set(_iter_borrowers_range(start, stop, s.rpc_max_log_range, s.allowed_reserves))
    added = _enqueue_many_newer(addrs, stop, s)
    return len(addrs), added


def _commit_window(s: BotState, head: int, mode: str, start: int, stop: int) -> None:
    if mode == FORWARD:
        s.last_scan_block = stop
        return
    if mode == BACKFILL:
        s.next_end = start - 1
        s.last_backfill_head = head
        return
    raise ValueError(f"unknown mode: {mode}")


def _sync_once(s: BotState, head: int, mode: str, max_windows: int = 1):
    if s is None:
        return {f"{mode}_skipped_no_state": 1}
    if not s.lock.acquire(timeout=0.2):
        return {f"{mode}_skipped_locked": 1}
    try:
        windows = 0
        uniq_total = 0
        added_total = 0
        first_start = None
        last_stop = None
        while windows < max_windows:
            plan = _plan_window(s, head, mode)
            if plan is None:
                break
            start, stop = plan

            uniq, added = _scan_window(s, start, stop)
            _commit_window(s, head, mode, start, stop)

            if first_start is None:
                first_start = start
            last_stop = stop

            windows += 1
            uniq_total += uniq
            added_total += added
        span = (last_stop - first_start + 1) if (first_start is not None) else 0
        return {
            f"{mode}_windows": windows,
            f"{mode}_first": first_start or 0,
            f"{mode}_last": last_stop or 0,
            f"{mode}_span": span,
            f"{mode}_unique": uniq_total,
            f"{mode}_new": added_total,
            "backlog_size": len(s.backlog),
            "seen_debtors_total": len(s.seen_borrow_block),
            "last_scan_block": s.last_scan_block,
            "next_end": s.next_end,
        }
    finally:
        s.lock.release()


def _process_live_borrow(s: BotState, log):
    debtor = _maybe_debtor(log, s.allowed_reserves)
    if not debtor:
        return {"borrow_event_ignored": 1}
    height = int(getattr(log, "block_number", s.last_scan_block))
    with _try_lock(s.lock, 0.2) as got:
        if not got:
            return {"borrow_event_skipped_locked": 1}
        added = _enqueue_many_newer({debtor}, height, s)
        return {
            "borrow_event": 1,
            "added_to_backlog": added,
            "backlog_size": len(s.backlog),
            "seen_debtors_total": len(s.seen_borrow_block),
        }


# Persistence
def _session_dir(bot_name: str | None = None) -> str:
    name = os.getenv("BOT_NAME", bot_name or "bot")
    eco = chain.provider.network.ecosystem.name
    net = chain.provider.network.name
    return os.path.join(".silverback-sessions", name, eco, net)


def _resolve_state_path(bot_name: str | None = None) -> str:
    p = os.getenv("STATE_PATH")
    if p:
        p = os.path.abspath(os.path.expanduser(p))
        if os.path.isdir(p):
            return os.path.join(p, "bot-state.json")
        return p
    return os.path.join(_session_dir(bot_name), "bot-state.json")


def _state_to_jsonable(s: BotState) -> dict:
    return {
        "next_end": s.next_end,
        "last_scan_block": s.last_scan_block,
        "seen_borrow_block": s.seen_borrow_block,
        "backlog": sorted(s.backlog),
    }


def _state_from_jsonable(d: dict) -> BotState:
    s = BotState()
    if "next_end" in d:
        s.next_end = d["next_end"]
    s.last_scan_block = int(d.get("last_scan_block", s.last_scan_block))
    s.seen_borrow_block = {k: int(v) for k, v in d.get("seen_borrow_block", {}).items()}
    s.backlog = set(d.get("backlog", []))
    return s


def _atomic_write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        f.flush()
    os.replace(tmp, path)


def _restore_state(path: str | None = None) -> tuple[BotState | None, str]:
    p = path or _resolve_state_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return _state_from_jsonable(payload), p
    except FileNotFoundError:
        return None, p
    except Exception as e:
        click.echo(f"[state] restore failed ({p}): {e}")
        return None, p


def _save_state(s: BotState, path: str | None = None) -> str:
    p = path or _resolve_state_path()
    try:
        _atomic_write_json(p, _state_to_jsonable(s))
    except Exception as e:
        click.echo(f"[state] snapshot failed ({p}): {e}")
    return p


# Silverback
@bot.on_startup()
def init_state(startup_state):
    s, path = _restore_state()
    restored = s is not None
    if s is None:
        s = BotState()
    s.allowed_reserves = set(RESERVES.values())
    head = chain.blocks.head.number
    if s.next_end is None:
        last = (
            startup_state.get("last_block_processed")
            if isinstance(startup_state, dict)
            else getattr(startup_state, "last_block_processed", None)
        )
        s.next_end = last or head
    if not restored:
        s.last_scan_block = head
    _cap_to_head(s, head)
    bot.state.data = s
    click.echo(
        f"[state] {'restored' if restored else 'fresh'} init; path={path}, next_end={s.next_end}"
    )


@bot.on_(chain.blocks)
def handle_blocks(b):
    s = bot.state.data
    head = b.number
    if head > s.last_scan_block:
        stats = _sync_once(s, head, FORWARD, max_windows=4)
        if s.last_scan_block < head:
            return {"catchup_in_progress": 1, **(stats or {})}
    if head - s.last_backfill_head < s.scan_interval_blocks:
        return
    return _sync_once(s, head, BACKFILL, max_windows=1)


@bot.on_(POOL.Borrow, filter_args={"reserve": list(RESERVES.values())})
def on_borrow(log):
    return _process_live_borrow(bot.state.data, log)


@bot.on_shutdown()
def handle_on_shutdown():
    s = bot.state.data
    path = _save_state(s)
    click.echo(f"[state] snapshot saved to: {path}")
    return {
        "backlog_size": len(s.backlog),
        "seen_debtors_total": len(s.seen_borrow_block),
        "state_saved": True,
    }
