# Baycat

Baycat is an efficiency-oriented S3 backup tool

It is currently in heavy development, so only use it if you're looking
for excitement and/or disappointment in your life.

## Motivation

baycat is optimized for local IOPs and AWS cost.  It's intended to
sync my homelab stack up to S3 for storage, but designed to allow
future extensions to handle other mirror-style operations.


## Sketch of Operation

The general idea here is to trust the filesystem metadata to only
change when the file changes, and use checksums to allow very quick
comparisons between local and remote state.  This is all stored in a
manifest file on the server as data is uploaded.  Subsequent update
checks are then very simple:

* Load the local manifest
* Walk local filesystem for anything which is different from the
  manifest file
* Download the remote manifest
* Optional: request bucket state from S3, to handle uploads which didn't update the manifest
* Compare local and remote manifest
* Compute a set of operations to synchronize the two
* Execute those ops as we can
* Update the remote manifest


# Usage

For basic usage, you can sync files with

`baycat sync /path/to/data s3://bucket/path/to/backup/`

You can manage manifests with the manifest subcommands:

To create a manifest without syncing anything, you can use

`baycat manifest create /path/to/data`

Which you can then update with

`baycat manifest update /path/to/data`

Both of these commands take a `--pool-size` argument so you can
compute the checksums in parallel as expected.  The `create` command
will save the chosen pool size in the saved manifest, while the
`update` command only uses the pool size for this specific instance.

Or if you want to stash the manifest file somewhere else

`baycat manifest create -o /tmp/sample_manifest /path/to/data`

See `--help` for each subcommand for other options.

# Random notes

## Development setup

baycat includes a `Makefile` to simplify working on it.  To set up a
dev environment, you can just do `make init` in the baycat directory.
This will set up a virtual environment with all the dependencies
needed for both running and testing baycat.  It also installs baycat
in the "editable" state in that venv.  This means you can edit the
library files and test things out without needing to reinstall the
package after every edit.

To use the virtual environment, you just need to run `source
.venv/bin/activate` in your shell to load it up.  To exit the virtual
environment, you just run `deactivate`.  (This works on bash;
virtualenv supports other shells via their own activate files in
`.venv/bin`.)

To run unit tests, use `make test` or `make coverage`.  The coverage
output will be in `htmlcov/index.html`.  This turns off all logging by
default, but the `BAYCAT_TEST_LOGLEVEL` environment variable can be
used to pass in a string for the logging library.  You will most
typically want to use this like

```
BAYCAT_TEST_LOGLEVEL=debug python3 -m unittest discover -s tests/ -k
TestS3Man
```

This will run all tests matching the pattern `TestS3Man` with logging
at full-on debug.

Finally, to find egregiously unpythonic file layouts, use `make
pycodestyle`.  Note that there are plenty of things it gets annoyed
about in the existing codebase, so the current linting guidelines are
"Please be tidy and consistent."  We may add proper linting to the
repo in the glorious future.



## Rationalization of the metadata storage

baycat solves the problem of storing unix metadata differently than
s3cmd does.  s3cmd uses some headers provided by S3 to stash the
attributes for later.  This would work for S3 (and make us s3cmd
compatible), but I envision baycat being expanded to support HTTP as a
download medium, so having the metadata in a file in the repo is a
better option.

## Administrivia

License: AGPL v3 <br/>
Author: Josh Myer <josh@joshisanerd.com> <br/>
Status: pre-alpha

README contents to follow.

Copyright (c) 2022-2023 Josh Myer <josh@joshisanerd.com>

