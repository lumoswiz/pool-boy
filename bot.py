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


# Backfill
CHUNK_BLOCKS = 100
SCAN_INTERVAL_BLOCKS = 5
RPC_MAX_LOG_RANGE = 10


def _iter_block_ranges(start_block: int, stop_block: int, step: int):
    cur = start_block
    while cur <= stop_block:
        end = min(cur + step - 1, stop_block)
        yield cur, end
        cur = end + 1


def _get_historical_borrows(start_block: int, stop_block: int):
    if stop_block < start_block:
        return
    allowed = bot.state.allowed_reserves
    for s, e in _iter_block_ranges(start_block, stop_block, RPC_MAX_LOG_RANGE):
        f = LogFilter(
            addresses=[POOL.address],
            events=[POOL.Borrow.abi],
            start_block=s,
            stop_block=e,
        )
        try:
            for log in accounts.provider.get_contract_logs(f):
                if log.reserve in allowed:
                    yield log
        except Exception as ex:
            click.echo(f"[get_logs] {s}-{e} failed: {ex}")
            continue


def _collect_borrowers_range(start_block: int, stop_block: int) -> Set[str]:
    debtors: Set[str] = set()
    for log in _get_historical_borrows(start_block, stop_block):
        debtors.add(log.onBehalfOf)
    return debtors


# Silverback
@bot.on_startup()
def init_state(startup_state=None):
    bot.state.allowed_reserves = set(RESERVES.values())
    bot.state.chunk_blocks = CHUNK_BLOCKS
    bot.state.scan_interval_blocks = SCAN_INTERVAL_BLOCKS
    bot.state.last_scan_block = 0

    bot.state.seen_debtors = set()
    bot.state.backlog = set()

    last = None
    if isinstance(startup_state, dict):
        last = startup_state.get("last_block_processed")
    else:
        last = getattr(startup_state, "last_block_processed", None)

    bot.state.next_end = last or chain.blocks.head.number
    bot.state.chunk_no = 0


@bot.on_(chain.blocks)
def handle_blocks(b):
    s = bot.state
    last = getattr(s, "last_scan_block", 0)
    if b.number - last < s.scan_interval_blocks:
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
    seen = getattr(bot.state, "seen_debtors", set())
    backlog = getattr(bot.state, "backlog", set())
    click.echo(f"[shutdown] seen_debtors: total={len(seen)}")
    click.echo(f"[shutdown] backlog: total={len(backlog)}")
