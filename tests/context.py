
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import baycat
from baycat.local_file import LocalFile


class BaycatTestCase(unittest.TestCase):
    '''Base class for all baycat test cases

    This is a pretty straigthforward set of helper methods that create
    a fresh directory for unit tests to work against, and manipulate
    at will.
    '''
    # A set of directories to create.  You can bum these down to just
    # the leaf directories in the structure, to simplify life a bit.
    LEAF_DIRS = ["a/b/c/d"]

    # These are the dummy files and their contents that we set up
    # every run.
    FILECONTENTS = [
            ("a/afile", "contents of afile", "79c36f925735a81867048aa3c3a87b93"),
            ("a/afile2", "more contents", "fc94a20a012e5014fc2ea79b4efcb97f"),
            ("a/b/bfile", "some content", "9893532233caff98cd083a116b013c0b"),
            ("a/b/bfile2", "you're never gonna guess", "31aeb6de5b580dc89d3e101260eccd87"),
            ("a/b/bfile3",
             "This is fine \U0001f525\U0001f525\U0001f436\u2615\ufe0f\U0001f525\U0001f525",
             "5cb7a8a7b77a0bdedc3f1a5ee7392743"),
        ]

    def _build_dirs(self, base_suffix, leaf_dirs, file_contents):
        dpath = os.path.join(self.base_dir, base_suffix)
        os.makedirs(dpath, exist_ok=True)

        for dirpath in leaf_dirs:
            os.makedirs(os.path.join(dpath, dirpath), exist_ok=True)

        for subpath, content, _ in file_contents:
            path = os.path.join(dpath, subpath)
            with open(path, "w+") as f:
                f.write(content)

        return dpath

    def setUp(self):
        '''Creates a temporary test directory, and populates it with the
        directories and files as expected.
        '''
        self.base_dir = tempfile.mkdtemp(prefix="baycat_unit_test-")

        self.test_dir = self._build_dirs("0", self.LEAF_DIRS, self.FILECONTENTS)

    def tearDown(self):
        shutil.rmtree(self.base_dir)

    def _get_lf(self, subpath=None, do_checksum=False):
        if subpath is None:
            subpath = self.FILECONTENTS[0][0]

        return LocalFile.for_path(self.test_dir, subpath, do_checksum=do_checksum)

    def _ith_path(self, i, tgt_dir=None, fileset=None):
        # Get the full path to the i'th test file

        tgt_dir = tgt_dir or self.test_dir
        fileset = fileset or self.FILECONTENTS

        return os.path.join(tgt_dir, fileset[i][0])
