import click
from ape import chain
from silverback import SilverbackBot

bot = SilverbackBot()


@bot.on_(chain.blocks)
def handle_blocks(b):
    click.echo(f"Handled block number: {b.number}")
