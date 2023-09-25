from collections import namedtuple

import logging
import math
import os
import time

from _md5 import md5

from .json_serdes import JSONSerDes

PATH_DUMMY_FILENAME = ".baycat_dir_metadata"


class ReservedNameException(Exception): pass
class TODOException(Exception): pass
class ChecksumMissingException(ValueError): pass
class ChecksumKindException(ValueError): pass
class PathMismatchException(ValueError): pass


class LocalFile(JSONSerDes):
    '''A single file (or directory) on the local disk

    This class encapsulates all the state information about the given
    file, including its metadata and checksum (if one has been
    computed).  This is also used to hold the metadata for
    directories.

    '''
    JSON_CLASSNAME = "LocalFile"

    TIME_RESOLUTION_NS = 1

    def __init__(self, root_path, rel_path, statinfo, cksum=None, cksum_type="MD5", is_dir=False):
        self._json_classname = self.JSON_CLASSNAME
        self.rel_path = rel_path
        self.path = os.path.abspath(os.path.join(root_path, self.rel_path))
        self.cksum = cksum
        self.cksum_type = cksum_type
        self.is_dir = is_dir

        self.collected = time.time()

        # When adding fields here, don't forget to implement them in
        # from_json_obj().

        # Diffable fields
        self.size = statinfo.st_size
        self.mtime_ns = statinfo.st_mtime_ns
        # self.cksum is also usable for this

        # Metadata we keep track of for restore
        self.metadata = {
            "uid": statinfo.st_uid,
            "gid": statinfo.st_gid,
            "mode": statinfo.st_mode,
            "atime_ns": statinfo.st_atime_ns,
        }

    def get_utime(self):
        '''Get the tuple used for os.utime()'''
        return (self.metadata["atime_ns"], self.mtime_ns)

    def delta(self, old_state, compare_checksums=False, force_checksum=False):
        '''This computes which fields are dirty, running checksums if needed

        * old_state: a LocalFile to compare against, from the old manifest
        * compare_checksums: set to True to skip checksum tests on mtime changes
        * force_checksum: force recompute of checksum even if we don't think it's needed (TODO)
        '''
        if self.rel_path != old_state.rel_path:
            raise PathMismatchException(f"LocalFile delta paths don't match!")

        if self.cksum_type != old_state.cksum_type:
            raise ChecksumKindException(f'Incommensurable checksum types, must regen manifest')

        if compare_checksums and old_state.cksum is None:
            raise ChecksumMissingException(f'Requested checksum comparison, but no checksum on old state')

        result = {}  # fieldname => is_dirty bool

        # Flag to let downstream know if we did this
        result["_recomputed_cksum"] = False

        result["size"] = self.size != old_state.size
        result["mtime_ns"] = abs(self.mtime_ns - old_state.mtime_ns) > self.TIME_RESOLUTION_NS-1

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

    @classmethod
    def _md5(cls, path, chunksize=32*1024):
        '''Internal helper method: compute the MD5 of the file at a given path
        '''
        hash = md5()
        with open(path, "rb") as f:
            while True:
                buf = f.read(chunksize)
                if not buf:
                    break
                hash.update(buf)
        return hash.hexdigest()

    def recompute_checksum(self):
        if self.is_dir:
            return
        self.cksum = self._md5(self.path)

    @classmethod
    def for_path(cls, root_path, rel_path, is_dir=False, do_checksum=False):
        '''Constructor method: generate a LocalFile for a given path

        root_path: the path to the directory this baycat manifest is rooted under
        rel_path: the path to the file/directory relative to root_path
        is_dir: a flag for whether or not is a directory
        do_checksum: set to True to request a checksum to be computed (otherwise it's left blank)
        '''
        path = os.path.join(root_path, rel_path)
        sr = os.stat(path, follow_symlinks=False)

        if is_dir:
            rel_path_out = rel_path
            while rel_path_out.endswith("/"):
                rel_path_out = rel_path_out[:-1]
        else:
            if path.endswith("/" + PATH_DUMMY_FILENAME):
                raise ReservedNameException(f"You have a file named {PATH_DUMMY_FILENAME}, which will not be synced")
            rel_path_out = rel_path

        cksum = None
        if do_checksum and not is_dir:
            cksum = cls._md5(path)

        result = LocalFile(root_path, rel_path_out, sr, cksum=cksum, is_dir=is_dir)
        return result

    @classmethod
    def for_abspath(cls, root_path, abs_path, **kwargs):
        '''
        '''

        if not abs_path.startswith(root_path):
            raise PathMismatchException(f"abs_path '{abs_path}' does not begin with root_path {root_path}")

        n = len(root_path)
        if not root_path.endswith("/"):
            n += 1

        return cls.for_path(root_path, abs_path[n:], **kwargs)

    def __str__(self):
        return f'{self.path}@{self.mtime_ns}/{self.size}'

    # No custom to_json_obj needed

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            logging.debug(f'Got a non-JSON-SerDes object in LocalFile!')
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in LocalFile!')
            raise ValueError(f'Got invalid object!')

        sr_d = dict()

        top_level = ["size", "mtime_ns"]
        md_level = ["uid", "gid", "mode", "atime_ns"]

        for f in top_level:
            sr_d[f'st_{f}'] = obj[f]

        for f in md_level:
            sr_d[f'st_{f}'] = obj["metadata"][f]

        SRDummy = namedtuple("SRDummy", sr_d.keys())

        sr_dummy = SRDummy(**sr_d)

        result = LocalFile(obj["root_path"], obj["rel_path"], sr_dummy,
                           obj["cksum"], obj["cksum_type"], is_dir=obj["is_dir"])
        result.collected = obj["collected"]

        return result

    def __eq__(self, b):
        cands = [self.__class__, super(self.__class__, self)] + self.__class__.__subclasses__()
        if b.__class__ not in cands:
            return NotImplemented

        my_fields = self.to_json_obj()
        b_fields = b.to_json_obj()

        def _dict_cmp(a, b):
            allkeys = set(a.keys()) | set(b.keys())

            for k in allkeys:
                if k in ["metadata", "atime_ns", "collected", "root_path", "path"]:
                    continue
                if a.get(k, None) != b.get(k, None):
                    if k == '_json_classname':
                        # We need to figure out subclassing in our name conventions.
                        continue
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

    def mark_contents_transferred(self, src_lf):
        '''Mark that we have transfered the contents from the given LocalFile

        This is used during the sync process, whenever a file is
        transfered over.

        NB: It is the responsibility of the caller to ensure that the
        checksum of the source file and our new copy match.
        '''
        self.cksum_type = src_lf.cksum_type
        self.cksum = src_lf.cksum
        self.size = src_lf.size

        self.metadata["atime_ns"], self.mtime_ns = src_lf.get_utime()

    def mark_metadata_transferred(self, src_lf):
        '''Mark that we transferred the metadata from the given LocalFile
        '''
        for f in ["uid", "gid", "mode"]:
            self.metadata[f] = src_lf.metadata[f]

        self.metadata["atime_ns"], self.mtime_ns = src_lf.get_utime()
