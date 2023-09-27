import logging
import os
import unittest


from context import baycat, BaycatTestCase

from baycat.cli_impl import CLIImpl
from baycat.manifest import Manifest


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
        m = self._get_test_manifest()

        tgt_dir = os.path.join(self.base_dir, "1")
        os.makedirs(tgt_dir, exist_ok=True)

        m2 = Manifest.for_path(tgt_dir)
        self.assertEqual(1, len(m2.entries))

        self.cli_impl.sync(self.test_dir, tgt_dir)

        m2.update()

        self.assertEquivalentDirs(self.test_dir, tgt_dir)

    def test_sync__s3_round_trip(self):
        dst_bucket = "dst"
        bucket = self._mk_bucket(dst_bucket)
        s3_url = f"s3://{dst_bucket}/some/path"

        tgt_dir = os.path.join(self.base_dir, "1")
        os.makedirs(tgt_dir, exist_ok=True)

        self.cli_impl.sync(self.test_dir, s3_url)
        self.cli_impl.sync(s3_url, tgt_dir)

        self.assertEquivalentDirs(self.test_dir, tgt_dir)

        tgt_dir2 = os.path.join(self.base_dir, "2")
        os.makedirs(tgt_dir2, exist_ok=True)
        self.cli_impl.sync(s3_url, tgt_dir2)
        self.assertEquivalentDirs(self.test_dir, tgt_dir2)
        self.assertEquivalentDirs(tgt_dir, tgt_dir2)

    def test_sync__s3_to_s3(self):
        self.assertRaises(ValueError, lambda: self.cli_impl.sync("s3://bucket/", "s3://bucket_too/"))
