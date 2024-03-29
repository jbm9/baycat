import logging
import os
import unittest

import boto3
from botocore.exceptions import ClientError

from context import baycat, BaycatTestCase

from baycat.cli_impl import CLIImpl
from baycat.manifest import Manifest
from baycat.util import bc_path_join


class TestCLIImpl(BaycatTestCase):
    def setUp(self):
        super(TestCLIImpl, self).setUp()
        self.cli_impl = CLIImpl()

    def test_is_s3(self):
        self.assertTrue(self.cli_impl.path_is_s3("s3://bucket/path"))
        self.assertTrue(self.cli_impl.path_is_s3("s3://bucket/"))
        self.assertTrue(self.cli_impl.path_is_s3("s3://"))  # Want bogons to trip later

        self.assertFalse(self.cli_impl.path_is_s3("http://slashdot.org"))
        self.assertFalse(self.cli_impl.path_is_s3("/etc/passwd"))
        self.assertFalse(self.cli_impl.path_is_s3("."))
        self.assertFalse(self.cli_impl.path_is_s3(""))

    def test_sync__local_to_local(self):
        tgt_dir = bc_path_join(self.base_dir, "1")
        os.makedirs(tgt_dir, exist_ok=True)
        self.cli_impl.sync(self.test_dir, tgt_dir)
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

    def test_sync__local_to_local__target_dne(self):
        tgt_dir = bc_path_join(self.base_dir, "1")
        self.cli_impl.sync(self.test_dir, tgt_dir)
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

    def test_sync__local_to_local__target_file(self):
        m = self._get_test_manifest()
        tgt_dir = bc_path_join(self.base_dir, "1")

        with open(tgt_dir, "w") as f:
            f.write("whomp whomp")

        self.assertRaises(FileExistsError, lambda:
                          self.cli_impl.sync(self.test_dir, tgt_dir))

    def test_sync__local_to_local__target_dne_dryrun(self):
        tgt_dir = bc_path_join(self.base_dir, "1")

        self.cli_impl.dry_run = True
        self.assertRaises(FileNotFoundError, lambda:
                          self.cli_impl.sync(self.test_dir, tgt_dir))

    def test_sync__s3_round_trip(self):
        dst_bucket = "dst"
        bucket = self._mk_bucket(dst_bucket)
        s3_url = f"s3://{dst_bucket}/some/path"

        tgt_dir = bc_path_join(self.base_dir, "1")

        m_upload = self.cli_impl.sync(self.test_dir, s3_url).manifest_xfer
        m_download = self.cli_impl.sync(s3_url, tgt_dir).manifest_xfer
        self.assertEquivalentDirs(self.test_dir, tgt_dir)

        tgt_dir2 = bc_path_join(self.base_dir, "2")
        self.cli_impl.sync(s3_url, tgt_dir2)
        self.assertEquivalentDirs(self.test_dir, tgt_dir2)
        self.assertEquivalentDirs(tgt_dir, tgt_dir2)

    def test_sync__s3_to_s3(self):
        self.assertRaises(ValueError, lambda: self.cli_impl.sync("s3://bucket/", "s3://bucket_too/"))

    def test_sync__to_s3_dne(self):
        dst_bucket = "dst_does_not_exist"
        s3_url = f"s3://{dst_bucket}/some/path"

        tgt_dir = bc_path_join(self.base_dir, "1")

        self.assertRaises(ClientError, lambda:
                          self.cli_impl.sync(self.test_dir, s3_url))

    def test_sync__efficiency(self):
        '''Ensure that the sync process does what we expect

        This is a quick check to verify that we aren't doing more work
        than needed for our transfers.  It's mostly a hedge against
        regressions.
        '''

        dst_bucket = "dst"
        bucket = self._mk_bucket(dst_bucket)
        dst_prefix = "some/path"
        s3_url = f"s3://{dst_bucket}/{dst_prefix}"

        tgt_dir = bc_path_join(self.base_dir, "1")

        sync1 = self.cli_impl.sync(self.test_dir, s3_url)
        self.assertEqual(len(self.FILECONTENTS),
                         sync1.manifest_dst.counters["s3_uploads"])

        sync1_rep = self.cli_impl.sync(self.test_dir, s3_url)
        self.assertEqual(0,
                         sync1_rep.manifest_dst.counters["s3_uploads"])

        # Ensure our manifest exists
        s3 = boto3.client("s3")
        entries = s3.list_objects_v2(Bucket=dst_bucket,
                                     Prefix=dst_prefix)
        expected_manifest_location = dst_prefix + "/.baycat/s3manifest"
        got_manifest = False
        for e in entries["Contents"]:
            if e["Key"] == expected_manifest_location:
                got_manifest = True

        self.assertTrue(got_manifest, entries["Contents"])

        m = self.cli_impl._load_manifest(s3_url)

        sync2 = self.cli_impl.sync(s3_url, tgt_dir)
        # Check that the sync was successful

        self.assertEquivalentDirs(self.test_dir, tgt_dir)
        self.assertEqual(1,
                         sync2.manifest_src.counters["s3_list_objects"])
        self.assertEqual(len(self.FILECONTENTS),
                         sync2.manifest_src.counters["s3_downloads"])

        sync3 = self.cli_impl.sync(s3_url, tgt_dir)
        # Check that the sync was successful
        self.assertEquivalentDirs(self.test_dir, tgt_dir)
        self.assertEqual(1,
                         sync3.manifest_src.counters["s3_list_objects"])
        self.assertEqual(0,
                         sync3.manifest_src.counters["s3_downloads"])
