from collections import namedtuple

import datetime
import logging
import math
import os
import time

from _md5 import md5

from .json_serdes import JSONSerDes

from .local_file import ReservedNameException, ChecksumMissingException, \
    ChecksumKindException, PathMismatchException, ReservedNameException


PATH_DUMMY_FILENAME = ".baycat_dir_metadata"


class S3File(JSONSerDes):
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

    '''
    JSON_CLASSNAME = "S3File"

    def __init__(self, root_path, obj_summary):
        '''Create a new S3File instance

        Parameters:
        * root_path: /prefix/to/base/directory/
        * obj_summary: the ObjectSummary from boto3

        Note that root_path
        '''
        self._json_classname = self.JSON_CLASSNAME
        self.root_path = root_path
        self.rel_path = self._s3path_to_rel(obj_summary["Key"])
        self.path = obj_summary["Key"]
        self.cksum = obj_summary["ETag"][1:-1]  # Remove the goofy quotes
        self.cksum_type = "MD5"
        self.is_dir = False

        self.collected = time.time()

        # When adding fields here, don't forget to implement them in
        # from_json_obj().

        # Diffable fields
        self.size = obj_summary["Size"]
        self.mtime_ns = 1e9 * obj_summary["LastModified"].timestamp()  # NB
        # self.cksum is also usable for this

        # Metadata we keep track of for restore
        self.metadata = {
            "uid": None,
            "gid": None,
            "mode": None,
            "atime_ns": None,
        }

    def _s3path_to_rel(self, p):
        '''Strip a long S3 path to just the relative path'''
        return p[len(self.root_path):]  # XXX TODO make this less awful

    def get_utime(self):
        '''Get the tuple used for os.utime()

        Note that this is *ONLY* S3 time, so not helpful on local
        filesystems!
        '''
        return (self.mtime_ns, self.mtime_ns)

    def delta(self, old_state, compare_checksums=False, force_checksum=False):
        '''This computes which fields are dirty, running checksums if needed

        * old_state: an S3File to compare against, from the old manifest
        * compare_checksums: set to True to skip checksum tests on mtime changes
        * force_checksum: force recompute of checksum even if we don't think it's needed (TODO)
        '''
        if self.rel_path != old_state.rel_path:
            raise PathMismatchException(f"S3File delta paths don't match!")

        if self.cksum_type != old_state.cksum_type:
            raise ChecksumKindException(f'Incommensurable checksum types, must regen manifest')

        if compare_checksums and old_state.cksum is None:
            raise ChecksumMissingException(f'Requested checksum comparison, but no checksum on old state')

        result = {}  # fieldname => is_dirty bool

        # Flag to let downstream know if we did this
        result["_recomputed_cksum"] = False

        result["size"] = self.size != old_state.size
        result["mtime_ns"] = abs(self.mtime_ns - old_state.mtime_ns) > 1e9

        if not compare_checksums:
            result["cksum"] = None
        else:
            if force_checksum or self.cksum is None:
                self.recompute_checksum()
                result["_recomputed_cksum"] = True

            result["cksum"] = self.cksum != old_state.cksum

        for f in self.metadata:
            if f == "atime_ns":
                result[f] = False
            else:
                result[f] = self.metadata[f] != old_state.metadata[f]

        # Add a flag to let us quickly check if anything has changed
        any_dirty = any(result.values())
        result["_dirty"] = any_dirty

        return result

    def recompute_checksum(self):
        return  # This is non-sensical on an S3 file

    def __str__(self):
        return f'{self.path}@{self.mtime_ns}/{self.size}'

    # No custom to_json_obj needed

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            logging.debug(f'Got a non-JSON-SerDes object in S3File!')
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in S3File!')
            raise ValueError(f'Got invalid object!')

        root_path = obj["root_path"]

        objsum = {
                "Key": os.path.join(root_path, obj["rel_path"]),
                "ETag": '"' + obj["cksum"] + '"',
                "Size": obj["size"],
                "LastModified": datetime.datetime.fromtimestamp(obj["mtime_ns"]/1e9,
                                                                datetime.timezone.utc),
            }

        return S3File(root_path, objsum)

    def __eq__(self, b):
        if self.__class__ != b.__class__:
            return False

        my_fields = self.to_json_obj()
        b_fields = b.to_json_obj()

        def _dict_cmp(a, b):
            allkeys = set(a.keys()) | set(b.keys())

            for k in allkeys:
                if k in ["metadata", "atime_ns", "collected", "root_path", "path"]:
                    continue
                if a.get(k, None) != b.get(k, None):
                    return False

            return True

        if not _dict_cmp(my_fields, b_fields):
            return False

        return _dict_cmp(my_fields.get("metadata", {}),
                         b_fields.get("metadata", {}))

    def changed_from(self, rhs, force_checksum=False):
        '''Returns a bool whether or not this file is changed compared to 'rhs'

        rhs: the file entry from another Manifest corresponding to this file
        force_checksum: force a full recompute of the local checksum (expensive)
        '''
        # Check metadata quickly
        if self.size != rhs.size or self.mtime_ns != rhs.mtime_ns:
            return True

        for k in self.metadata.keys():
            if k == "atime_ns":
                continue
            if self.metadata[k] != rhs.metadata[k]:
                return True

        # If we're forced to update our checksum, do so
        if force_checksum:
            self.recompute_checksum()

        # And now actually work with checksum data
        if self.cksum is None:
            return False

        if self.cksum != rhs.cksum:
            return True

        return False

    def mark_contents_transferred(self, src_s3f):
        '''Mark that we have transfered the contents from the given S3File

        This is used during the sync process, whenever a file is
        transfered over.

        NB: It is the responsibility of the caller to ensure that the
        checksum of the source file and our new copy match.
        '''
        self.cksum_type = src_s3f.cksum_type
        self.cksum = src_s3f.cksum
        self.size = src_s3f.size

        self.metadata["atime_ns"], self.mtime_ns = src_s3f.get_utime()

    def mark_metadata_transferred(self, src_s3f):
        '''Mark that we transferred the metadata from the given S3File
        '''
        for f in ["uid", "gid", "mode"]:
            self.metadata[f] = src_s3f.metadata[f]

        self.metadata["atime_ns"], self.mtime_ns = src_s3f.get_utime()
