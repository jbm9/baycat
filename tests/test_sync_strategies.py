import json
import logging
import os
import unittest
import shutil
import time
import tempfile

from context import baycat, BaycatTestCase

from baycat.file_selectors import PathSelector
from baycat.manifest import Manifest
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder

from baycat.sync_strategies import SyncLocalToLocal


class TestSyncLocalToLocal(BaycatTestCase):

    def _copy_test_dir_via_sll(self):
        m = Manifest()
        ps = PathSelector(self.test_dir)
        m.add_selector(ps)

        tgt_dir = os.path.join(self.base_dir, "1")
        time.sleep(0.01)
        os.makedirs(tgt_dir, exist_ok=True)

        m2 = Manifest()
        ps2 = PathSelector(tgt_dir)
        m2.add_selector(ps2)

        sll = SyncLocalToLocal(m, m2)

        sll.sync()
        return tgt_dir

    def assertEquivalentManifests(self, p1, p2):
        '''Assert that p1 and p2 result in equal manifests'''
        m1 = Manifest.for_path(p1)
        m2 = Manifest.for_path(p2)

        diffs = m1.diff_from(m2)

        for k in ["added", "deleted", "contents", "metadata"]:
            self.assertEqual(0, len(diffs[k]),
                             f'{k}: {diffs[k]}')

    def test_full_copy(self):
        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentManifests(self.test_dir, tgt_dir)

    def test_noop(self):
        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentManifests(self.test_dir, tgt_dir)

        m1 = Manifest.for_path(self.test_dir)
        m2 = Manifest.for_path(tgt_dir)
        sll = SyncLocalToLocal(m1, m2)
        m_got = sll.sync()

        self.assertEqual(m1, m_got)
        self.assertEquivalentManifests(self.test_dir, tgt_dir)

    def _mangle_target_dir(self, tgt_dir):
        ##############################
        # Make a bunch of changes

        time.sleep(0.01)  # sleep to force changes in timestamps
        delete_path = self._ith_path(0)
        rewrite_path = self._ith_path(1)
        new_path = self._ith_path(0) + "-but-new"
        chmod_path = self._ith_path(2)

        os.unlink(delete_path)

        with open(rewrite_path, "w") as f:
            f.write("your mom was here")

        with open(new_path, "w+") as f:
            f.write("stuff")

        os.chmod(chmod_path, 0o0600)

        expected_counts = {
            "added": 1,
            "deleted": 1,
            "contents": 1,
            "metadata": 1+1,
        }

        return expected_counts

    def test_diff_from__changes(self):
        # Note that this is a bit inside-out: we create a copy of the
        # test directory, then modify it.  But we then treat the
        # original directory as the "new" state, and try to sync back
        # to it.

        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentManifests(self.test_dir, tgt_dir)

        # And then change stuff
        expected_n = self._mangle_target_dir(tgt_dir)

        ##############################
        # Check that the modifications happened
        m_orig = Manifest.for_path(self.test_dir)
        m_after = Manifest.for_path(tgt_dir)

        got = m_orig.diff_from(m_after)
        for k, n_exp in expected_n.items():
            self.assertEqual(n_exp, len(got[k]),
                             f'{k}: {got[k]}')

        self.assertEqual(got["new_manifest"], m_orig)
        self.assertEqual(got["old_manifest"], m_after)

        ##############################
        # Now apply the diff and ensure it restores state
        sll = SyncLocalToLocal(m_orig, m_after)
        sll.sync()

        m_orig = Manifest.for_path(self.test_dir)
        m_restored = Manifest.for_path(tgt_dir)

        got = m_orig.diff_from(m_restored)

        self.assertEqual(0, len(got["added"]))
        self.assertEqual(1, len(got["deleted"]))  # Since we disabled it
        self.assertEqual(0, len(got["contents"]))
        self.assertEqual(0, len(got["metadata"]))

        self.assertEqual(got["new_manifest"], m_orig)
        self.assertEqual(got["old_manifest"], m_restored)

    def test_diff_from__changes_with_del(self):
        # Note that this is a bit inside-out: we create a copy of the
        # test directory, then modify it.  But we then treat the
        # original directory as the "new" state, and try to sync back
        # to it.

        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentManifests(self.test_dir, tgt_dir)

        # And then change stuff
        expected_n = self._mangle_target_dir(tgt_dir)

        ##############################
        # Check that the modifications happened
        m_orig = Manifest.for_path(self.test_dir)
        m_after = Manifest.for_path(tgt_dir)

        got = m_orig.diff_from(m_after)
        for k, n_exp in expected_n.items():
            self.assertEqual(n_exp, len(got[k]),
                             f'{k}: {got[k]}')

        self.assertEqual(got["new_manifest"], m_orig)
        self.assertEqual(got["old_manifest"], m_after)

        ##############################
        # Now apply the diff and ensure it restores state
        sll = SyncLocalToLocal(m_orig, m_after, enable_delete=True)
        sll.sync()

        m_orig = Manifest.for_path(self.test_dir)
        m_restored = Manifest.for_path(tgt_dir)

        got = m_orig.diff_from(m_restored)

        self.assertEqual(0, len(got["added"]))
        self.assertEqual(0, len(got["deleted"]))
        self.assertEqual(0, len(got["contents"]))
        self.assertEqual(0, len(got["metadata"]))

        self.assertEqual(got["new_manifest"], m_orig)
        self.assertEqual(got["old_manifest"], m_restored)

    def test_dry_run__copy(self):
        # Make a destination directory, and capture its state
        tgt_dir = os.path.join(self.base_dir, "tgt/x/y")
        os.makedirs(tgt_dir)
        m_tgt_orig = Manifest.for_path(tgt_dir)

        # Run a dry run sync to it
        m1 = Manifest.for_path(self.test_dir)
        m2 = Manifest.for_path(tgt_dir)
        sll = SyncLocalToLocal(m1, m2, dry_run=True)
        sll.sync()

        m_tgt_post = Manifest.for_path(tgt_dir)
        self.assertEqual(m_tgt_orig, m_tgt_post)

    def test_dry_run__delete_disabled(self):
        # Make a destination directory, and capture its state
        empty_dir = os.path.join(self.base_dir, "tgt/x/y")
        os.makedirs(empty_dir)
        m_test_orig = Manifest.for_path(self.test_dir)

        # Run a dry run sync to it
        m1 = Manifest.for_path(empty_dir)
        m2 = Manifest.for_path(self.test_dir)
        sll = SyncLocalToLocal(m1, m2, dry_run=True)
        sll.sync()

        m_test_post = Manifest.for_path(self.test_dir)
        self.assertEqual(m_test_orig, m_test_orig)

    def test_dry_run__delete_enabled(self):
        # Make a destination directory, and capture its state
        empty_dir = os.path.join(self.base_dir, "tgt/x/y")
        os.makedirs(empty_dir)
        m_test_orig = Manifest.for_path(self.test_dir)

        # Run a dry run sync to it
        m1 = Manifest.for_path(empty_dir)
        m2 = Manifest.for_path(self.test_dir)
        sll = SyncLocalToLocal(m1, m2, dry_run=True, enable_delete=True)
        sll.sync()

        m_test_post = Manifest.for_path(self.test_dir)
        self.assertEqual(m_test_orig, m_test_orig)

