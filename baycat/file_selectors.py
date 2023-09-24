from abc import ABC, abstractmethod

import os
import logging

from .local_file import LocalFile, ReservedNameException
from .json_serdes import JSONSerDes


class FileSelector(ABC):
    '''Interface definition for the FileSelector classes.

    These can take arbitrary constructor input, but should implement a
    walk() method which is a generator that yields LocalFile objects.
    '''

    @abstractmethod
    def walk(self):
        yield LocalFile.for_path("/etc", "passwd")


class PathSelector(FileSelector, JSONSerDes):
    '''Selects all files under a given root path

    This implements a walk() iterator, which yields a LocalFile object
    for every file in the selector.
    '''

    JSON_CLASSNAME = "PathSelector"

    def __init__(self, rootpath):
        self.rootpath = rootpath
        self._json_classname = self.JSON_CLASSNAME

    def walk(self):
        yield LocalFile.for_path(self.rootpath, ".", is_dir=True)
        len_rootpath = len(self.rootpath)

        for root, dirs, files in os.walk(self.rootpath):
            for name in files:
                path = os.path.join(root, name)
                try:
                    yield LocalFile.for_abspath(self.rootpath, path)
                except ReservedNameException:
                    logging.warning(f'Local file "{path}" conflicts with directory metadata file')
                    pass

            for name in dirs:
                path = os.path.join(root, name)
                yield LocalFile.for_abspath(self.rootpath, path, is_dir=True)

    def to_json_obj(self):
        result = {
            "_json_classname": self.JSON_CLASSNAME,
            "rootpath": self.rootpath,
        }
        return result

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in PathSelector!')
            raise ValueError(f'Got invalid object!')

        result = PathSelector(rootpath=obj["rootpath"])
        return result

    def __eq__(self, rhs):
        if self.__class__ != rhs.__class__:
            return NotImplemented

        return self.rootpath == rhs.rootpath
