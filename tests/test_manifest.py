import logging
import json
import os
import unittest
import shutil
import tempfile
import time

from context import baycat, BaycatTestCase

from baycat.manifest import Manifest, DifferentRootPathException, \
    ManifestAlreadyExists, VacuousManifestError
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestManifest(BaycatTestCase):
    def test_reserved_path(self):
        m = Manifest(root="/tmp/foo")
        self.assertEqual(m.reserved_prefix, ".baycat/")

        m = Manifest(root="/tmp/foo", path="/tmp/foo/bar")
        self.assertEqual(m.reserved_prefix, "bar/")

        m = Manifest(root="/tmp/foo", path="/tmp/bar")
        self.assertEqual(m.reserved_prefix, None)

    def test__entries_keys(self):
        m = self._get_test_manifest()

        keys_got = set(m.entries.keys())
        keys_expected = set([a[0] for a in self.FILECONTENTS])

        keys_expected.add('')  # Add the root dir

        for ld in self.LEAF_DIRS:   # And add the other directories
            components = ld.split("/")
            cur_path = ""
            for dirname in components:
                cur_path += dirname
                keys_expected.add(cur_path)
                cur_path += "/"

        self.assertEqual(keys_got, keys_expected)

    def test_is_reserved_path(self):
        m = self._get_test_manifest()
        self.assertTrue(m.is_reserved_path(".baycat/manifest)"))
        self.assertTrue(m.is_reserved_path(".baycat/randomcruft)"))
        self.assertFalse(m.is_reserved_path(".baycat_foo"))

        m = Manifest(root="/tmp/foo")
        self.assertTrue(m.is_reserved_path(".baycat/manifest)"))
        self.assertTrue(m.is_reserved_path(".baycat/randomcruft)"))
        self.assertFalse(m.is_reserved_path(".baycat_foo"))

        m = Manifest(root="/tmp/foo", path="/tmp/foo/bar")
        self.assertTrue(m.is_reserved_path("bar/manifest)"))
        self.assertTrue(m.is_reserved_path("bar/randomcruft)"))
        self.assertTrue(m.is_reserved_path("bar/.baycat_foo"))
        self.assertFalse(m.is_reserved_path(".baycat/manifest"))

        m = Manifest(root="/tmp/foo", path="/tmp/bar")
        self.assertFalse(m.is_reserved_path(".baycat/manifest)"))
        self.assertFalse(m.is_reserved_path(".baycat/randomcruft)"))
        self.assertFalse(m.is_reserved_path(".baycat_foo"))

    def test_save_path(self):
        m = Manifest.for_path(self.test_dir)
        m.save()

        expected_path = os.path.join(self.test_dir, ".baycat", "manifest")
        self.assertTrue(os.path.exists(expected_path))
        self.assertTrue(os.path.isfile(expected_path))

    def test_default_manifest_path(self):
        self.assertEqual(Manifest._default_manifest_path("/tmp/dummy"),
                         "/tmp/dummy/.baycat/manifest")

    def test_init__no_args(self):
        self.assertRaises(ValueError, lambda: Manifest(path=None, root=None))

    def test_update__no_selectors(self):
        m = Manifest(root="/tmp/foobar")
        self.assertRaises(VacuousManifestError, lambda: m.update())

    def test_save__no_selectors(self):
        m = Manifest(root="/tmp/foobar")
        self.assertRaises(VacuousManifestError, lambda: m.save())

    def test_serdes__happy_path(self):
        m = self._get_test_manifest()
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
        m = self._get_test_manifest()
        m.save()
        m_round_trip = Manifest.load(self.test_dir)

        self.assertEqual(m, m_round_trip)

    def test_save_load__overwrite(self):
        target_dir = os.path.join(self.test_dir, "test_manifest")
        os.makedirs(target_dir)

        filename = os.path.join(self.test_dir, "test_manifest/.manifest")
        dummy_str = "Overwrite me"
        with open(filename, "w") as f:
            f.write(dummy_str)

        m = Manifest.for_path(self.test_dir)

        self.assertRaises(ManifestAlreadyExists,
                          lambda: m.save(manifest_path=filename))

        # Even with overwrite, we're not going to delete a file to make our dir
        self.assertRaises(ValueError,
                          lambda: m.save(path=filename, overwrite=True))

        m.save(manifest_path=filename, overwrite=True)

        m2 = Manifest.load(self.test_dir, filename)
        self.assertEqual(m, m2)

    def test_eq__bogons(self):
        target_path = os.path.join(self.test_dir, "a")
        m = Manifest.for_path(target_path)

        m2 = Manifest(root=self.test_dir)

        self.assertNotEqual(m, 0)
        self.assertNotEqual(m, m2)

    def test_eq__happy_path(self):
        target_path = os.path.join(self.test_dir, "a")
        ps = self._get_ps("a")

        m = Manifest(root=target_path)
        m.add_selector(ps)

        m2 = Manifest(root=target_path)
        m2.add_selector(ps)

        self.assertEqual(m, m2)

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
        ps = self._get_ps("a")
        m = Manifest.for_path(os.path.join(self.test_dir, "a"))

        m_copy = m.copy()
        self.assertEqual(m, m_copy)
        m_copy.update()
        self.assertEqual(m, m_copy)

        mpath = os.path.join(self.test_dir, "confirm_path")
        m2 = Manifest(path=mpath)
        m2.add_selector(ps)

        got = m.diff_from(m2)

        for k in ["added", "deleted", "contents", "metadata"]:
            self.assertEqual(0, len(got[k]), k)

        self.assertEqual(got["new_manifest"], m)
        self.assertEqual(got["old_manifest"], m2)

    def test_diff_from__changes(self):
        dpath = os.path.join(self.test_dir, "a")
        m = Manifest.for_path(dpath)
        self.assertTrue(len(m.selectors))

        m_orig_copy = m.copy()

        self.assertTrue(len(m_orig_copy.selectors))
        self.assertEqual(m, m_orig_copy)

        time.sleep(0.01)  # a tiny blip to force diffs in timestamps

        delete_path = self._ith_path(0)
        delete_name = self._ith_name(0)[2:]

        rewrite_path = self._ith_path(1)
        rewrite_name = self._ith_name(1)[2:]

        new_path = self._ith_path(0) + "-but-new"
        new_name = self._ith_name(0)[2:] + "-but-new"

        chmod_path = self._ith_path(2)
        chmod_name = self._ith_name(2)[2:]

        regress_path = self._ith_path(3)
        regress_name = self._ith_name(3)[2:]

        os.unlink(delete_path)

        with open(rewrite_path, "w") as f:
            f.write("your mom was here")

        with open(new_path, "w+") as f:
            f.write("stuff")

        os.chmod(chmod_path, 0o0600)

        ut = m.entries[regress_name].get_utime()
        with open(regress_path, "w+") as f:
            f.write("your grandma was here")
        dt = 86400
        utp = (ut[0]-dt, ut[1]-dt)
        os.utime(regress_path, ns=utp)

        # Now build a new manifest
        mpath = os.path.join(self.test_dir, "confirm_path")
        m_new = Manifest(path=mpath)
        ps = self._get_ps("a")
        m_new.add_selector(ps)

        got = m_new.diff_from(m)

        self.assertEqual([delete_name], got["deleted"])
        self.assertEqual([new_name], got["added"])
        self.assertEqual(set([rewrite_name, regress_name]),
                         set(got["contents"]))
        self.assertEqual(set(['', chmod_name]),
                         set(got["metadata"]))
        self.assertEqual(set([regress_name]),
                         set(got["regressed"]))

        self.assertEqual(got["new_manifest"], m_new)
        self.assertEqual(got["old_manifest"], m)

        # And let's test update() while we're here
        self.assertNotEqual(m_orig_copy, m_new)
        m_orig_copy.update()
        self.assertEqual(m_orig_copy, m_new)

    def test_add_selector__twice_is_noop(self):
        ps = self._get_ps("a")

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
        ps = self._get_ps("a")

        mpath = os.path.join(self.test_dir, ".baycat_manifest")

        m = Manifest(path="_")
        m.add_selector(ps)

        ps2 = self._get_ps("a")
        m.add_selector(ps2)

        ps3 = self._get_ps("a/b")
        self.assertRaises(DifferentRootPathException, lambda: m.add_selector(ps3))

    def test_add_selector__checksums(self):
        ps = self._get_ps()

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
        ps = self._get_ps()

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
