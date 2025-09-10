import os
from dataclasses import dataclass, field
from typing import Set

import click
from ape import Contract, accounts, chain
from ape.types import LogFilter
from silverback import SilverbackBot

bot = SilverbackBot()

# Addresses
POOL = Contract("0xA238Dd80C259a72e81d7e4664a9801593F98d1c5")

RESERVES = {
    "WETH": "0x4200000000000000000000000000000000000006",
    "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
    "cbBTC": "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
    "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
}

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


def _get_historical_borrows(start_block: int, stop_block: int):
    if stop_block < start_block:
        return

    s = bot.state.data
    allowed = s.allowed_reserves
    step = s.rpc_max_log_range

    for start, stop in _iter_block_ranges(start_block, stop_block, step):
        f = LogFilter(
            addresses=[POOL.address],
            events=[POOL.Borrow.abi],
            start_block=start,
            stop_block=stop,
        )
        try:
            for log in accounts.provider.get_contract_logs(f):
                if log.reserve in allowed:
                    yield log
        except Exception as ex:
            click.echo(f"[get_logs] {start}-{stop} failed: {ex}")
            continue


def _collect_borrowers_range(start_block: int, stop_block: int) -> Set[str]:
    debtors: Set[str] = set()
    for log in _get_historical_borrows(start_block, stop_block):
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

    chunk_debtors = _collect_borrowers_range(start, stop)
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
