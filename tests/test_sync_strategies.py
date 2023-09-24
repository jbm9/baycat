from io import BytesIO
import json
import logging
import os
import unittest
import shutil
import time
import tempfile

import boto3

from context import baycat, BaycatTestCase

from baycat.file_selectors import PathSelector
from baycat.manifest import Manifest
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder
from baycat.s3_manifest import S3Manifest
from baycat.sync_strategies import SyncLocalToLocal, SyncLocalToS3, SyncS3ToLocal


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

    def test_full_copy(self):
        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

    def test_noop(self):
        tgt_dir = self._copy_test_dir_via_sll()
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

        m1 = Manifest.for_path(self.test_dir)
        m2 = Manifest.for_path(tgt_dir)
        sll = SyncLocalToLocal(m1, m2)
        m_got = sll.sync()

        self._assert_counters(sll)  # shouldn't be anything here.

        self.assertEqual(m1, m_got)
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

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
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

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
        self._assert_counters(sll, xfer=2, xfer_metadata=2+4, delete_skipped=1)

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
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

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
        self._assert_counters(sll, xfer=2, xfer_metadata=2+4, rm=1)

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
        self._assert_counters(sll)  # dry run: nothing happened

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
        self._assert_counters(sll, delete_skipped=9)  # dry run, ignore non-actions

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
        self._assert_counters(sll, delete_skipped=9)

        m_test_post = Manifest.for_path(self.test_dir)
        self.assertEqual(m_test_orig, m_test_orig)


class TestSyncLocalToS3(BaycatTestCase):
    def test__smoke_test(self):
        m = Manifest()
        ps = PathSelector(self.test_dir)
        m.add_selector(ps)

        dst_bucket = "dst"
        bucket = self._mk_bucket(dst_bucket)
        m2 = S3Manifest.from_bucket(dst_bucket)
        self.assertEqual(len(m2.entries), 0)

        sls3 = SyncLocalToS3(m, m2)

        sls3.sync()

        m_got = S3Manifest.from_bucket(dst_bucket)
        orig_files = [ lf for lf in m.entries.values() if not lf.is_dir ]
        self.assertEqual(len(m_got.entries), len(orig_files))

        ########################################
        # Now we add some files locally and try it again.

        self._build_dirs("0", ["z"], [("z/zfile", "whee", "eaaff0f66f4f3bc0a1acc9820b3666de")])
        m_new = Manifest()
        ps = PathSelector(self.test_dir)
        m_new.add_selector(ps)

        new_files = [ lf for lf in m_new.entries.values() if not lf.is_dir ]
        self.assertNotEqual(len(orig_files), len(new_files))
        self.assertNotEqual(len(m_got.entries), len(new_files))

        sls3_new = SyncLocalToS3(m_new, m_got)
        sls3_new.sync()

        m_got_new = S3Manifest.from_bucket(dst_bucket)
        self.assertEqual(len(m_got_new.entries), len(new_files))

        ########################################
        # And now we touch a file and see what happens
        i_mangle = 0
        p = self._ith_path(i_mangle)
        rel_p = self.FILECONTENTS[i_mangle][i_mangle]
        s3_p = os.path.join(m_got.root, rel_p)
        new_content = "changed contents"

        with open(p, "w") as fh:
            fh.write(new_content)

        m_mangled = Manifest()
        ps = PathSelector(self.test_dir)
        m_mangled.add_selector(ps)

        mangled_files = [ lf for lf in m_mangled.entries.values() if not lf.is_dir ]
        self.assertEqual(len(mangled_files), len(new_files))

        sls3_mangled = SyncLocalToS3(m_mangled, m_got)
        sls3_mangled.sync()

        m_got_mangled = S3Manifest.from_bucket(dst_bucket)
        self.assertEqual(len(m_got_mangled.entries), len(mangled_files))

        s3 = boto3.client("s3")
        result_fh = BytesIO()
        s3.download_fileobj(dst_bucket, s3_p, result_fh)
        got_contents = result_fh.getvalue().decode("UTF8")
        self.assertEqual(got_contents, new_content)


class TestSyncS3ToLocal(BaycatTestCase):
    '''Strictly speaking, this is actually a round-trip test.
    '''
    def test__smoke_test(self):
        logging.basicConfig(level=logging.DEBUG)

        m = Manifest()
        ps = PathSelector(self.test_dir)
        m.add_selector(ps)

        dst_bucket = "dst"
        bucket = self._mk_bucket(dst_bucket)
        m2 = S3Manifest.from_bucket(dst_bucket)

        self.assertEqual(len(m2.entries), 0)

        sls3 = SyncLocalToS3(m, m2)
        m2p = sls3.sync()
        m2p.save()

        self.assertNotEqual(len(m2p.entries), 0)

        m_got = S3Manifest.load(dst_bucket, "/")

        ########################################
        # Now sync it back down.

        tgt_dir = os.path.join(self.base_dir, "1")
        time.sleep(0.01)
        os.makedirs(tgt_dir, exist_ok=True)

        m_tgt = Manifest()
        ps_tgt = PathSelector(tgt_dir)
        m_tgt.add_selector(ps_tgt)

        ss3l = SyncS3ToLocal(m_got, m_tgt)
        ss3lp = ss3l.sync()

        ########################################
        # And now create a new manifest from the target dir
        m_tgt_got = Manifest()
        ps_tgt_got = PathSelector(tgt_dir)
        m_tgt_got.add_selector(ps_tgt_got)

        diffs = m.diff_from(m_tgt_got)

        for k in ["added", "deleted", "contents", "metadata"]:
            self.assertEqual(0, len(diffs[k]),
                             f'{k}: {diffs[k]}')
