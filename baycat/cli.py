#!/usr/bin/env python3

import logging
import os
import sys
from traceback import print_exception

import click

from .cli_impl import CLIImpl

import boto3
from moto import mock_s3

cli_impl = CLIImpl()


@click.group()
@click.option('--log-level', default=None)
def cli(log_level):
    if 'BAYCAT_TEST_LOGLEVEL' in os.environ:
        log_level = os.environ['BAYCAT_TEST_LOGLEVEL']
        logging.basicConfig(level=log_level.upper())

    # The following are Chatty Cathies.
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)

    if log_level:
        logging.basicConfig(level=log_level.upper())


@cli.command()
@click.argument('src')
@click.argument('dst')
@click.option('-n', '--dry-run', is_flag=True,
              help="Dry run (doesn't change anything on disk or transfer files)")
@click.option('-v', '--verbose', is_flag=True,
              help="Verbose output")
@click.option('-q', '--quiet', is_flag=True,
              help="Quiets down everything but errors and warnings")
@click.option('--maybe-spend-money', is_flag=True,
              help="Don't enable the moto mock of S3")
def sync(src, dst, verbose, dry_run, quiet, maybe_spend_money):
    cli_impl.verbose = verbose
    cli_impl.dry_run = dry_run

    if not maybe_spend_money:
        mock_s3().start()
        s3 = boto3.resource("s3")
        bucket_name = "foo"
        bucket = s3.create_bucket(Bucket=bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ur-butt-1'})

    if dry_run:
        # XXX TODO Only make this change if they haven't explicitly set it
        logging.basicConfig(level="INFO")

    try:
        ss = cli_impl.sync(src, dst)
        total_size = sum([e.size for e in ss.manifest_xfer.entries.values()])
        bytes_up = 1 + ss.manifest_xfer.counters["bytes_up"]
        rho = total_size/bytes_up
        if not quiet:
            print(f'Uploaded {bytes_up} vs repo of {total_size},  speedup of {rho:0.3f}')
    except Exception as e:
        print(f'Problem with run: {e}')
        if verbose:
            print_exception(e)


if __name__ == '__main__':
    cli()
