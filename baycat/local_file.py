from collections import namedtuple

import logging
import os
import time

from .json_serdes import JSONSerDes

PATH_DUMMY_FILENAME = ".baycat_dir_metadata"


class ReservedNameException(Exception): pass
class TODOException(Exception): pass
class ChecksumMissingException(ValueError): pass
class ChecksumKindException(ValueError): pass
class PathMismatchException(ValueError): pass

class LocalFile(JSONSerDes):
    JSON_CLASSNAME = "LocalFile"

    def __init__(self, path, statinfo, cksum=None, cksum_type="MD5"):
        self._json_classname = self.JSON_CLASSNAME
        self.path = path
        self.cksum = cksum
        self.cksum_type = cksum_type

        self.collected = time.time()

        # When adding fields here, don't forget to implement them in
        # from_json_obj().

        # Diffable fields
        self.size = statinfo.st_size
        self.mtime = statinfo.st_mtime
        # self.cksum is also usable for this

        # Metadata we keep track of for restore
        self.metadata = {
            "uid": statinfo.st_uid,
            "gid": statinfo.st_gid,
            "mode": statinfo.st_mode,
            "atime": statinfo.st_atime,
            "ctime": statinfo.st_ctime,
        }

    def delta(self, old_state, compare_checksums=False, force_checksum=False):
        '''This computes which fields are dirty, running checksums if needed

        * old_state: a LocalFile to compare against, from the old manifest
        * compare_checksums: set to True to skip checksum tests on mtime changes
        * force_checksum: force recompute of checksum even if we don't think it's needed (TODO)
        '''
        if self.path != old_state.path:
            raise PathMismatchException(f"LocalFile delta paths don't match!")

        if self.cksum_type != old_state.cksum_type:
            raise ChecksumKindException(f'Incommensurable checksum types, must regen manifest')

        if compare_checksums and old_state.cksum is None:
            raise ChecksumMissingException(f'Requested checksum comparison, but no checksum on old state')

        result = {}  # fieldname => is_dirty bool

        # Flag to let downstream know if we did this
        result["_recomputed_cksum"] = False

        result["size"] = self.size != old_state.size
        result["mtime"] = self.mtime != old_state.mtime

        if not compare_checksums:
            result["cksum"] = None
        else:
            if force_checksum or self.cksum is None:
                result["_recomputed_cksum"] = True
                raise TODOException("Implement checksums")

            result["cksum"] = self.cksum != old_state.cksum

        for f in self.metadata:
            result[f] = self.metadata[f] != old_state.metadata[f]

        # Add a flag to let us quickly check if anything has changed
        any_dirty = any(result.values())
        result["_dirty"] = any_dirty

        return result

    @classmethod
    def for_path(cls, path, is_dir=False):
        sr = os.stat(path, follow_symlinks=False)

        if is_dir:
            path_out = os.path.join(path, PATH_DUMMY_FILENAME)
        else:
            if path.endswith("/" + PATH_DUMMY_FILENAME):
                raise ReservedNameException(f"You have a file named {PATH_DUMMY_FILENAME}, which will not be synced")
            path_out = path

        return LocalFile(path_out, sr, cksum=None)

    def __str__(self):
        return f'{self.path}@{self.mtime}/{self.size}'

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

        top_level = ["size", "mtime"]
        md_level = ["uid", "gid", "mode", "atime", "ctime"]

        for f in top_level:
            sr_d[f'st_{f}'] = obj[f]

        for f in md_level:
            sr_d[f'st_{f}'] = obj["metadata"][f]

        SRDummy = namedtuple("SRDummy", sr_d.keys())

        sr_dummy = SRDummy(**sr_d)

        result = LocalFile(obj["path"], sr_dummy, obj["cksum"], obj["cksum_type"])
        result.collected = obj["collected"]

        return result

    def __eq__(self, b):
        if self.__class__ != b.__class__:
            return False

        my_fields = self.to_json_obj()
        b_fields = b.to_json_obj()

        def _dict_cmp(a, b):
            allkeys = set(a.keys()) | set(b.keys())

            for k in allkeys:
                if k == "metadata":
                    continue
                if a.get(k, None) != b.get(k, None):
                    return False

            return True

        if not _dict_cmp(my_fields, b_fields):
            return False

        return _dict_cmp(my_fields.get("metadata", {}),
                         b_fields.get("metadata", {}))
