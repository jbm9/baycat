import logging
import json
import os

from .json_serdes import JSONSerDes, BaycatJSONEncoder, baycat_json_decoder

MANIFEST_FILENAME = ".baycat_manifest"


class Manifest(JSONSerDes):
    JSON_CLASSNAME = "Manifest"

    def __init__(self, path=MANIFEST_FILENAME):
        self.path = path
        self.entries = {}  # path => LocalFile

    def add_selector(self, sel):
        '''Add from the file_selector interface selector given'''

        for lf in sel.walk():
            self.entries[lf.path] = lf

    def save(self, path=None, overwrite=False):
        if path is None:
            path = self.path

        if not overwrite and os.path.exists(path):
            raise ValueError(f"There is already a manifest at {path} and you didn't specify overwriting")

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

        result = Manifest(path=obj["path"])
        result.entries = obj["entries"]
        return result

    def diffs(self, old_manifest):
        '''Collects a set of file diffs between the two manifests.'''
