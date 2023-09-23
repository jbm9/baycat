from collections import namedtuple

import datetime
import logging
import math
import os
import time

from _md5 import md5

from .json_serdes import JSONSerDes

from .local_file import LocalFile, ReservedNameException, ChecksumMissingException, \
    ChecksumKindException, PathMismatchException, ReservedNameException


PATH_DUMMY_FILENAME = ".baycat_dir_metadata"


class S3File(LocalFile):
    '''A single file (or directory) in an S3 bucket

    This class encapsulates the available state information about the
    given path.  Note that some metadata is not captured by S3
    (uid/gid), so those are somewhat lost.

    NB: Unlike LocalFile, this does *not* capture data for directories!

    We treat the bucket as if it's just a standard filesystem and use
    standard path conventions out of of more or less laziness.  The
    bucket name is totally lost to S3Files, but that's fine, as the
    bucket is their filesystem.

    Note that s3objsum is a boto3.resources.factory.s3.ObjectSummary
    object, created via boto3's s3.list_objects_v2() helper.

    Data which is lost/incomplete:

    * mtime: tracks S3 access, not FS access (so useless)
    * uid: not tracked in S3
    * gid: not tracked in S3
    * mode: not tracked in S3
    * atime_ns: not tracked in S3

    We therefore have to save this all in the metadata file itself.

    '''
    JSON_CLASSNAME = "S3File"
    TIME_RESOLUTION_NS = 1000000000

    @classmethod
    def from_objsum(cls, root_path, obj_summary):
        cksum = obj_summary["ETag"].strip('"')
        cksum_type = "MD5"

        is_dir = False

        # Unwind our slightly janky directory placeholders
        key = obj_summary["Key"]
        if False and os.path.basename(key) == S3MANIFEST_FILENAME:
            is_dir = True
            key = os.path.dirname(key)

        rel_path = key[len(root_path):]  # XXX TODO make this less awful

        st_size = obj_summary["Size"]
        st_mtime_ns = 1e9 * obj_summary["LastModified"].timestamp()  # NB
        st_atime_ns = st_mtime_ns
        st_uid = None
        st_gid = None
        st_mode = None

        SI = namedtuple("SI", "st_size st_mtime_ns st_uid st_gid st_mode st_atime_ns")
        statinfo = SI(st_size, st_mtime_ns, st_uid, st_gid, st_mode, st_atime_ns)

        return S3File(root_path, rel_path, statinfo, cksum, cksum_type, is_dir)

    def recompute_checksum(self):
        return  # This is non-sensical on an S3 file
