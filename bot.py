import click
from ape import Contract, chain
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


@bot.on_(chain.blocks)
def handle_blocks(b):
    click.echo(f"Handled block number: {b.number}")
