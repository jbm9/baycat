import logging
import json
import os
import unittest
import shutil
import tempfile

from context import baycat

from baycat.file_selectors import PathSelector
from baycat.manifest import Manifest
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestManifest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _get_lf(self, path="/etc/passwd"):
        return LocalFile.for_path(path)

    def test_serdes__happy_path(self):
        os.makedirs(os.path.join(self.test_dir, "a/b/c/d"), exist_ok=True)

        files = [
            ("a/afile", "contents of afile"),
            ("a/afile2", "more contents"),
            ("a/b/bfile", "some content"),
            ("a/b/bfile2", "you're never gonna guess"),
        ]

        for subpath, content in files:
            path = os.path.join(self.test_dir, subpath)
            with open(path, "w+") as f:
                f.write(content)

        ps = PathSelector(self.test_dir)
        m = Manifest()
        m.add_selector(ps)

        m_json = m.to_json()

        m_round_trip = json.loads(m_json, object_hook=baycat_json_decoder)

        self.assertEqual(m, m_round_trip)

    def test_serdes__bogons(self):
        self.assertRaises(ValueError, lambda: Manifest.from_json_obj({"how": "many"}))

        self.assertRaises(ValueError,
                          lambda: Manifest.from_json_obj({"_json_classname": "YourMom"}))

    def test_save_load__happy_path(self):
        os.makedirs(os.path.join(self.test_dir, "a/b/c/d"), exist_ok=True)

        files = [
            ("a/afile", "contents of afile"),
            ("a/afile2", "more contents"),
            ("a/b/bfile", "some content"),
            ("a/b/bfile2", "you're never gonna guess"),
        ]

        for subpath, content in files:
            path = os.path.join(self.test_dir, subpath)
            with open(path, "w+") as f:
                f.write(content)

        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m.save()

        m_round_trip = Manifest.load(mpath)

        self.assertEqual(m, m_round_trip)

    def test_eq__bogons(self):
        os.makedirs(os.path.join(self.test_dir, "a/b/c/d"), exist_ok=True)

        files = [
            ("a/afile", "contents of afile"),
            ("a/afile2", "more contents"),
            ("a/b/bfile", "some content"),
            ("a/b/bfile2", "you're never gonna guess"),
        ]

        for subpath, content in files:
            path = os.path.join(self.test_dir, subpath)
            with open(path, "w+") as f:
                f.write(content)

        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m2 = Manifest()

        self.assertNotEqual(m, 0)
        self.assertNotEqual(m, m2)


if __name__ == '__main__':
    unittest.main()
