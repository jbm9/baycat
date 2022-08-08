from abc import ABC, abstractmethod

import os
import logging

from .local_file import LocalFile, ReservedNameException


class FileSelector(ABC):
    '''Interface definition for the FileSelector classes.

    These can take arbitrary constructor input, but should implement a
    walk() method which is a generator that yields LocalFile objects.
    '''

    @abstractmethod
    def walk(self):
        yield LocalFile.for_path("/etc", "passwd")


class PathSelector(FileSelector):
    '''Selects all files under a given root path

    This implements a walk() iterator, which yields a LocalFile object
    for every file in the selector.
    '''

    def __init__(self, rootpath):
        self.rootpath = rootpath

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
