import os
from dataclasses import dataclass, field

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


# Backfill
def _iter_block_ranges(start_block: int, stop_block: int, step: int):
    cur = start_block
    while cur <= stop_block:
        end = min(cur + step - 1, stop_block)
        yield cur, end
        cur = end + 1


def _get_historical_borrows(
    start_block: int,
    stop_block: int,
    allowed_reserves: set[str],
    step: int,
):
    if stop_block < start_block:
        return

    for start, stop in _iter_block_ranges(start_block, stop_block, step):
        f = LogFilter(
            addresses=[POOL.address],
            events=[POOL.Borrow.abi],
            start_block=start,
            stop_block=stop,
        )
        try:
            for log in accounts.provider.get_contract_logs(f):
                if log.reserve in allowed_reserves:
                    yield log
        except Exception as ex:
            click.echo(f"[get_logs] {start}-{stop} failed: {ex}")
            continue


def _collect_borrowers_range(
    start_block: int,
    stop_block: int,
    allowed_reserves: set[str],
    step: int,
) -> set[str]:
    debtors: set[str] = set()
    for log in _get_historical_borrows(start_block, stop_block, allowed_reserves, step):
        debtors.add(log.onBehalfOf)
    return debtors


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

    if s.next_end is None:
        s.next_end = b.number

    start = max(1, s.next_end - s.chunk_blocks + 1)
    stop = s.next_end

    chunk_debtors = _collect_borrowers_range(
        start,
        stop,
        s.allowed_reserves,
        s.rpc_max_log_range,
    )
    new_debtors = chunk_debtors - s.seen_debtors
    s.seen_debtors |= chunk_debtors
    s.backlog |= new_debtors

    click.echo(
        f"[backfill] chunk #{s.chunk_no} {start}-{stop} "
        f"-> {len(chunk_debtors)} in chunk, {len(new_debtors)} new, total {len(s.seen_debtors)}"
    )

    s.next_end = start - 1
    s.chunk_no += 1
    s.last_scan_block = b.number


@bot.on_shutdown()
def handle_on_shutdown():
    s = bot.state.data
    seen = s.seen_debtors if s else set()
    backlog = s.backlog if s else set()
    click.echo(f"[shutdown] seen_debtors: total={len(seen)}")
    click.echo(f"[shutdown] backlog: total={len(backlog)}")
