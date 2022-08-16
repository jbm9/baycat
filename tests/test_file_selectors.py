import json
import os
import unittest
import shutil
import tempfile

from context import baycat, BaycatTestCase

from baycat.file_selectors import PathSelector
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestFileSelectors(BaycatTestCase):
    def test_walk__happy_path(self):
        expected_dirs = [ "a", "a/b", "a/b/c", "a/b/c/d", ]

        expected_paths = []
        expected_paths += [ f'{s}' for s in expected_dirs ]
        expected_paths += [ f'{p}' for p, _, __ in self.FILECONTENTS ]

        expected_paths = [ os.path.join(self.test_dir, p) for p in expected_paths ]
        expected_paths.append(self.test_dir)

        got_paths = []

        ps = PathSelector(self.test_dir)

        for lf in ps.walk():
            got_paths.append(lf.path)

        self.assertEqual(sorted(got_paths), sorted(expected_paths))

    def test_walk__reservedname(self):
        with open(os.path.join(self.test_dir, "a/b/.baycat_dir_metadata"), "w+") as f:
            f.write("foo")

        expected_dirs = ["a", "a/b", "a/b/c", "a/b/c/d", ]

        expected_paths = []
        expected_paths += [ f'{s}' for s in expected_dirs ]
        expected_paths += [ f'{p}' for p, _, __ in self.FILECONTENTS ]

        expected_paths = [ os.path.join(self.test_dir, p) for p in expected_paths ]
        expected_paths.append(self.test_dir)

        got_paths = []

        ps = PathSelector(self.test_dir)

        for lf in ps.walk():
            got_paths.append(lf.path)

        self.assertEqual(sorted(got_paths), sorted(expected_paths))
