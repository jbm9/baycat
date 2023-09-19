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

TODO.

# Random notes

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

