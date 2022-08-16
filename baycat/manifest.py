import logging
import json
import os

from .json_serdes import JSONSerDes, BaycatJSONEncoder, baycat_json_decoder
from .file_selectors import PathSelector
from .local_file import LocalFile, PATH_DUMMY_FILENAME

MANIFEST_FILENAME = ".baycat_manifest"


class DifferentRootPathException(ValueError):
    '''Raised when an add_selection() can't be added to existing entries'''
    pass

class ManifestAlreadyExists(ValueError):
    pass

class Manifest(JSONSerDes):
    JSON_CLASSNAME = "Manifest"

    def __init__(self, path=MANIFEST_FILENAME, root=None):
        self.path = path
        self.root = root
        self.entries = {}  # path => LocalFile

    def add_selector(self, sel, do_checksum=False):
        '''Add from the file_selector interface selector given'''

        if self.root is None:
            self.root = sel.rootpath
        else:
            if self.root != sel.rootpath:
                errstr = f"Manifest at {self.root}, path selector at {sel.rootpath}"
                raise DifferentRootPathException(errstr)

        for lf in sel.walk():
            lf.recompute_checksum()
            self.entries[lf.rel_path] = lf

    @classmethod
    def for_path(cls, rootpath, path=MANIFEST_FILENAME, do_checksum=False):
        ps = PathSelector(rootpath)
        result = Manifest(path=path)
        result.add_selector(ps, do_checksum=do_checksum)
        return result

    def _expand_path(self, rel_path):
        return os.path.join(self.root, rel_path)

    def save(self, path=None, overwrite=False):
        if path is None:
            path = self.path

        if not overwrite and os.path.exists(path):
            raise ManifestAlreadyExists(f"There is already a manifest at {path} and you didn't specify overwriting")

        with open(path, "w+") as f:
            json.dump(self, f, default=BaycatJSONEncoder.default)

    @classmethod
    def load(self, path):
        with open(path, "r") as f:
            result = json.load(f, object_hook=baycat_json_decoder)

        return result

    def __eq__(self, m_b):
        if self.__class__ != m_b.__class__:
            return False

        # self.path is not checked here, as we can store the same
        # manifest in multiple locations.

        # Similarly, self.root is not checked

        if len(self.entries) != len(m_b.entries):
            return False

        all_paths = set(self.entries.keys()) | set(m_b.entries.keys())

        if len(all_paths) != len(self.entries):
            return False

        for p in all_paths:
            lf_s = self.entries[p]
            lf_b = m_b.entries[p]

            if lf_s != lf_b:
                return False

        return True

    def to_json_obj(self):
        result = {
            "_json_classname": self.JSON_CLASSNAME,
            "path": self.path,
            "root": self.root,
            "entries": {}
        }

        for p, lf in self.entries.items():
            result["entries"][p] = lf.to_json_obj()

        return result

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            logging.debug(f'Got a non-JSON-SerDes object in Manifest!')
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in Manifeste!')
            raise ValueError(f'Got invalid object!')

        result = Manifest(path=obj["path"], root=obj["root"])
        result.entries = obj["entries"]
        for k, v in result.entries.items():
            if v.__class__ == dict:
                result.entries[k] = baycat_json_decoder(v)
        return result

    def diff_from(self, old_manifest):
        '''Collects a set of file diffs from the given manifest to this one.

        This treats the current manifest as the goal state, with
        old_manifest as the state on-disk.

        Note that this doesn't do any actual work, it just finds what
        work needs to be done at this particular point in time.

        '''

        files_added = []    # Files present now, but missing then
        files_deleted = []  # Files present in the other manifest, but not here

        files_content_changed = []  # Files with content changes
        files_metadata_changed = []  # Files with metadata changes

        files_unchanged = []  # Files that have no changes

        # NB: we're going to use *relative* paths in this codepath,
        # not the absolute paths we use everywhere else.
        paths_self = set(self.entries.keys())
        paths_old = set(old_manifest.entries.keys())

        files_added = list(paths_self - paths_old)
        files_deleted = list(paths_old - paths_self)

        files_common = paths_self & paths_old

        for p in files_common:
            lf_new = self.entries[p]
            lf_old = old_manifest.entries[p]

            file_delta = lf_new.delta(lf_old)

            if not file_delta["_dirty"]:
                files_unchanged.append(p)
                continue

            contents_modified = False
            metadata_modified = False

            for k in ["size", "mtime_ns", "cksum"]:
                if file_delta[k]:
                    if lf_new.is_dir:
                        metadata_modified = True
                    else:
                        contents_modified = True

            for k in lf_new.metadata:
                if k == "atime_ns":
                    continue
                if file_delta[k]:
                    metadata_modified = True

            if contents_modified:
                files_content_changed.append(p)

            if metadata_modified:
                files_metadata_changed.append(p)

        #####################
        # We now have enough information to plan a sync.

        result = {
            "added": files_added,
            "deleted": files_deleted,
            "contents": files_content_changed,
            "metadata": files_metadata_changed,
            "unchanged": files_unchanged,
            "new_manifest": self,
            "old_manifest": old_manifest,
        }

        return result

    def mark_deleted(self, rel_path):
        del self.entries[rel_path]


    def mark_mkdir(self, rel_p):
        dpath = rel_p
        abspath = self._expand_path(rel_p)
        # Just make a new entry, this is cheap on local FS
        self.entries[dpath] = LocalFile.for_abspath(self.root, abspath)
