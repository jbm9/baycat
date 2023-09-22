#!/usr/bin/env python3

import logging
import sys

import click


@click.group()
def cli():
    pass


@cli.command()
def hellow():
    print("Heeelllloooo")

if __name__ == '__main__':
    cli()
