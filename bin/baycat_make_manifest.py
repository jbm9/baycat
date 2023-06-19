#!/usr/bin/env python3

import logging
from optparse import OptionParser
import os.path

from baycat.manifest import Manifest, MANIFEST_FILENAME, ManifestAlreadyExists

parser = OptionParser()

parser.add_option("-d", "--dirpath", default=".",
                  help="Directory to build manifest for (default is '.')",
                  metavar="DIRPATH")

parser.add_option("-o", "--output", default=None,
                  help="Output path for the manifest (default is DIRPATH/.baycat_manifest)",
                  metavar="PATH")

parser.add_option("-w", "--overwrite", action='store_true', default=False,
                  help="Enable overwriting an existing manifest file")

parser.add_option("--poolsize", type=int, default=None, metavar="N",
                  help="Number of checksum subprocesses to spawn (default=# of CPUs)")

parser.add_option("--debug", action='store_true', default=False)


(options, args) = parser.parse_args()

if options.debug:
    logging.basicConfig(level=logging.DEBUG)

logging.debug(f'options: {options}')

manifest_path = options.output or os.path.join(options.dirpath,
                                                MANIFEST_FILENAME)

logging.debug(f'Manifest path: {manifest_path}')
try:
    m = Manifest.for_path(options.dirpath,
                          path=manifest_path,
                          do_checksum=True,
                          poolsize=options.poolsize)
    m.save(overwrite=options.overwrite)
except ManifestAlreadyExists:
    logging.error(f"Manifest already exists.  Re-run with --overwrite (-w).")
