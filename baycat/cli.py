#!/usr/bin/env python3

import json
import logging
import os
import sys
from traceback import print_exception

import click

from .manifest import Manifest
from .cli_impl import CLIImpl

import boto3

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
@click.option('--print-counters', is_flag=True,
              help="Print counters after finishing the transfer")
def sync(src, dst, verbose, dry_run, quiet, maybe_spend_money, print_counters):
    '''Sync from src to dst, with the given options

    This returns 0 to the shell on success, 1 if there were errors
    during transfers, and 2 for unhandled exceptions.

    Note that we currently very loosely define our error handling
    during transfers, so it may be overly broad.
    '''
    cli_impl.verbose = verbose
    cli_impl.dry_run = dry_run

    if not maybe_spend_money:
        from moto import mock_s3
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

        if print_counters:
            print(f'Sync Strat counters: {dict(ss._counters)}')
            print(f'Manifest src counters: {dict(ss.manifest_src.counters)}')
            print(f'Manifest xfer counters: {dict(ss.manifest_xfer.counters)}')
            print(f'Manifest dst counters: {dict(ss.manifest_dst.counters)}')

        if ss.was_success():
            sys.exit(0)
        sys.exit(1)
    except Exception as e:
        print(f'Problem with run: {e}')
        if verbose:
            print_exception(e)
        sys.exit(2)


@cli.group()
def manifest():
    '''Manage manifest files'''
    pass


@manifest.command()
@click.argument('root_path')
@click.option('-m', '--manifest', default=None,
              help='Path to save the manifest at (default is root_path/.baycat_manifest')
@click.option('-c', '--pool-size', default=None,
              help='Number of subprocesses to use when computing checksums')
@click.option('--skip-checksums', is_flag=True)
def create(root_path, manifest, pool_size, skip_checksums):
    '''Create a new manifest for the given directory
    '''
    m = Manifest.for_path(root_path, path=manifest,
                          poolsize=pool_size, do_checksum=not skip_checksums)
    m.save()


@manifest.command()
@click.argument('root_path')
@click.option('-m', '--manifest', default=None,
              help='Path to save the manifest at (default is root_path/.baycat_manifest')
@click.option('-c', '--pool-size', default=None, type=int,
              help='Number of subprocesses to use when computing checksums (does not persist)')
@click.option('--force-checksums', is_flag=True)
def update(root_path, manifest, pool_size, force_checksums):
    '''Create a new manifest for the given directory
    '''
    m = Manifest.load(root_path, manifest)
    old_poolsize = m.poolsize
    if pool_size is not None:
        m.poolsize = pool_size
    m.update(force_checksums)
    m.poolsize = old_poolsize

    m.save(overwrite=True)


@manifest.command()
@click.argument('root_path')
@click.option('-m', '--manifest',
              help='Path to load the manifest from (default is root_path/.baycat_manifest')
@click.option('--storage-price', type=float, default=0.023,
              help='Price per gigabyte per month (ignores discounts)')
@click.option('--transaction-cost', type=float, default=0.005,
              help='Price per 1k requests (ignores discounts)')
@click.option('--fraction-changed', type=float, default=0.05,
             help='Fraction of the files which are changed each month')
@click.option('--syncs-per-month', type=int, default=24*30,
              help='Number of syncs per month')
@click.option('--download-cost', type=float, default=0.09,
              help='Cost per gigabyte downloaded (ignores discounts)')
def estimate_cost(root_path, manifest, storage_price,
                  transaction_cost, fraction_changed,
                  syncs_per_month, download_cost):
    m = Manifest.load(root_path, manifest)
    total_size = 0
    for e in m.entries.values():
        total_size += e.size if not e.is_dir else 0

    cost_to_upload = len(m.entries)/1000 * transaction_cost
    storage_per_mo = total_size/(1 << 30) * storage_price
    update_per_mo = fraction_changed * cost_to_upload

    s = json.dumps(m.to_json_obj())
    bytes_per_manifest = len(s)
    del(s)

    sync_dl_cost = bytes_per_manifest * syncs_per_month / (2<<30) * download_cost

    cost_per_mo = storage_per_mo + update_per_mo + sync_dl_cost

    print(f'Initial upload: ${cost_to_upload:0.4f}')
    print(f'Total size: {total_size} bytes, storage cost of ${storage_per_mo:0.4f}/mo')
    print(f'At {fraction_changed*100:0.1f}% changed/month, estimate update cost of ${update_per_mo:0.4f}')
    print(f'Doing {syncs_per_month} syncs/mo, expect ${sync_dl_cost:0.4f}/mo in overhead')
    print(f'Total (very rough) estimated monthly cost: ${cost_per_mo:0.4f}/mo')

if __name__ == '__main__':
    cli()
