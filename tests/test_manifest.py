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
        m = Manifest(path="_")
        m.add_selector(ps)

        m_json = m.to_json_obj()

        m_round_trip = Manifest.from_json_obj(m_json)

        self.assertEqual(m, m_round_trip)

    def test_serdes__bogons(self):
        self.assertRaises(ValueError, lambda: Manifest.from_json_obj({"how": "many"}))

        self.assertRaises(ValueError,
                          lambda: Manifest.from_json_obj({"_json_classname": "YourMom"}))

    def test_copy(self):
        m = Manifest.for_path(self.test_dir)

        m_copy = m.copy()
        self.assertEqual(m, m_copy)

        lf_copy = list(m_copy.entries.values())[0]
        lf_copy.mtime_ns = int(time.time())
        self.assertNotEqual(m, m_copy, m.diff_from(m_copy))

    def test_save_load__happy_path(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m.save()
        m_round_trip = Manifest.load(self.test_dir)

        self.assertEqual(m, m_round_trip)

    def test_save_load__overwrite(self):
        filename = os.path.join(self.test_dir, "test_manifest")
        dummy_str = "Overwrite me"
        with open(filename, "w") as f:
            f.write(dummy_str)

        m = Manifest.for_path(self.test_dir)

        self.assertRaises(ValueError, lambda: m.save(filename))

        m.save(filename, overwrite=True)

        m2 = Manifest.load(self.test_dir, filename)
        self.assertEqual(m, m2)

    def test_eq__bogons(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path=mpath)
        m.add_selector(ps)

        m2 = Manifest(path="_")

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

        m_copy = m.copy()
        self.assertEqual(m, m_copy)
        m_copy.update()
        self.assertEqual(m, m_copy)

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
        self.assertTrue(len(m.selectors))

        m_orig_copy = m.copy()

        self.assertTrue(len(m_orig_copy.selectors))
        self.assertEqual(m, m_orig_copy)

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

        self.assertNotEqual(m_orig_copy, m2)
        m_orig_copy.update()
        self.assertEqual(m_orig_copy, m2)

    def test_add_selector__twice_is_noop(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path="_")
        m.add_selector(ps)

        m2 = Manifest(path="_")
        m2.add_selector(ps)

        self.assertEqual(m, m2)

        m2 = Manifest(path="_")
        m2.add_selector(ps)
        m2.add_selector(ps)

        self.assertEqual(m, m2)

    def test_add_selector__different_roots(self):
        ps = PathSelector(os.path.join(self.test_dir, "a"))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path="_")
        m.add_selector(ps)

        ps2 = PathSelector(os.path.join(self.test_dir, "a"))
        m.add_selector(ps2)

        ps3 = PathSelector(os.path.join(self.test_dir, "a", "b"))
        self.assertRaises(DifferentRootPathException, lambda: m.add_selector(ps3))

    def test_add_selector__checksums(self):
        ps = PathSelector(os.path.join(self.test_dir, ""))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")
        logging.basicConfig(level=logging.DEBUG)

        m = Manifest(path="_", poolsize=1)  # Keep it in the single process for test coverage
        m.add_selector(ps, do_checksum=True)

        n_checked = 0
        for rp, _, cksum in self.FILECONTENTS:
            self.assertEqual(cksum, m.entries[rp].cksum, rp)
            n_checked += 1

        # Double-check that we haven't screwed up and NO-OPed
        self.assertTrue(bool(n_checked))

    def test_add_selector__no_checksums(self):
        ps = PathSelector(os.path.join(self.test_dir, ""))

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path="_")
        m.add_selector(ps, do_checksum=False)

        n_checked = 0
        for rp, _, cksum in self.FILECONTENTS:
            self.assertEqual(None, m.entries[rp].cksum, rp)
            n_checked += 1

        # Double-check that we haven't screwed up and NO-OPed
        self.assertTrue(bool(n_checked))

    def test_for_path__checksums(self):
        m = Manifest.for_path(self.test_dir, do_checksum=True, poolsize=1)

        n_checked = 0
        for rp, _, cksum in self.FILECONTENTS:
            self.assertEqual(cksum, m.entries[rp].cksum, rp)
            n_checked += 1

        # Double-check that we haven't screwed up and NO-OPed
        self.assertTrue(bool(n_checked))
