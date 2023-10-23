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

        ps = self._get_ps()

        for lf in ps.walk():
            got_paths.append(lf.path)

        self.assertEqual(sorted(got_paths), sorted(expected_paths))

    def test_eq(self):
        ps = self._get_ps()
        ps2 = self._get_ps()
        ps3 = self._get_ps("/bin/")

        self.assertEqual(ps, ps)
        self.assertEqual(ps, ps2)
        self.assertEqual(ps2, ps)

        self.assertNotEqual(ps, ps3)
        self.assertNotEqual(ps3, ps)

    def test_json_round_trip(self):
        ps = self._get_ps()

        ps2 = ps.copy()

        self.assertEqual(ps, ps2)

