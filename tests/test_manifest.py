import logging
import json
import os
import unittest
import shutil
import tempfile
import time

from context import baycat, BaycatTestCase

from baycat.file_selectors import PathSelector
from baycat.manifest import Manifest, DifferentRootPathException
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestManifest(BaycatTestCase):
    def test_serdes__happy_path(self):
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
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m.save()

        m_round_trip = Manifest.load(mpath)

        self.assertEqual(m, m_round_trip)

    def test_save_load__overwrite(self):
        filename = os.path.join(self.test_dir, "test_manifest")
        dummy_str = "Overwrite me"
        with open(filename, "w") as f:
            f.write(dummy_str)

        m = Manifest.for_path(self.test_dir)

        self.assertRaises(ValueError, lambda: m.save(filename))

        m.save(filename, overwrite=True)

        m2 = Manifest.load(filename)
        self.assertEqual(m, m2)

    def test_eq__bogons(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m2 = Manifest()

        self.assertNotEqual(m, 0)
        self.assertNotEqual(m, m2)

    def test_eq__happy_path(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

    def test_eq__mismatch_pathset(self):
        m = Manifest.for_path(self.test_dir)

        p0 = self._ith_path(0)
        os.rename(p0, p0+"-lolmoved")

        m2 = Manifest.for_path(self.test_dir)

        self.assertNotEqual(m, m2)

    def test_eq__mismatch_entry(self):
        m = Manifest.for_path(self.test_dir)

        p0 = self._ith_path(0)
        with open(p0, "w") as f:
            f.write("This got changed")

        m2 = Manifest.for_path(self.test_dir)

        self.assertNotEqual(m, m2)

    def test_diff_from__nochanges(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m2 = Manifest(path=mpath+"2")
        m2.add_selector(ps)

        got = m.diff_from(m2)

        for k in ["added", "deleted", "contents", "metadata"]:
            self.assertEqual(0, len(got[k]), k)

        self.assertEqual(got["new_manifest"], m)
        self.assertEqual(got["old_manifest"], m2)

    def test_diff_from__changes(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        def _ith_path(i):
            # Get the full path to the i'th test file
            return os.path.join(self.test_dir, self.FILECONTENTS[i][0])

        time.sleep(0.01)  # a tiny blip to force diffs in timestamps
        delete_path = _ith_path(0)
        rewrite_path = _ith_path(1)
        new_path = _ith_path(0) + "-but-new"
        chmod_path = _ith_path(2)

        os.unlink(delete_path)

        with open(rewrite_path, "w") as f:
            f.write("your mom was here")

        with open(new_path, "w+") as f:
            f.write("stuff")

        os.chmod(chmod_path, 0o0600)

        m2 = Manifest(path=mpath+"2")
        m2.add_selector(ps)

        got = m.diff_from(m2)

        self.assertEqual(1, len(got["added"]))
        self.assertEqual(1, len(got["deleted"]))
        self.assertEqual(1, len(got["contents"]))
        self.assertEqual(2, len(got["metadata"]))

        self.assertEqual(got["new_manifest"], m)
        self.assertEqual(got["old_manifest"], m2)

    def test_add_selector__twice_is_noop(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest()
        m.add_selector(ps)

        m2 = Manifest()
        m2.add_selector(ps)

        self.assertEqual(m, m2)

        m2 = Manifest()
        m2.add_selector(ps)
        m2.add_selector(ps)

        self.assertEqual(m, m2)

    def test_add_selector__different_roots(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest()
        m.add_selector(ps)

        ps2 = PathSelector(os.path.join(self.test_dir, "a"))
        m.add_selector(ps2)

        ps3 = PathSelector(os.path.join(self.test_dir, "a", "b"))
        self.assertRaises(DifferentRootPathException, lambda: m.add_selector(ps3))


if __name__ == '__main__':
    unittest.main()
