import logging
import json
import os
import unittest
import shutil
import tempfile
import time

import boto3
from moto import mock_s3

from context import baycat, BaycatTestCase

from baycat.s3_manifest import S3Manifest, ClientError


class TestS3Manifest(BaycatTestCase):
    def test_from_bucket(self):
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)
        self.assertEqual(len(m.entries), len(self.FILECONTENTS))

        for relpath, contents, md5sum in self.FILECONTENTS:
            s3f = m.entries[relpath]
            self.assertEqual(str(md5sum), s3f.cksum)

    def test_copy(self):
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)
        m2 = m.copy()

        self.assertEqual(m, m2)

    def test_from_bucket__nobucket(self):
        self.assertRaises(ClientError,
                          lambda: S3Manifest.from_bucket("yourmom", "/"))

    def test_from_bucket__empty(self):
        empty_bucket = "empty"
        self._mk_bucket(empty_bucket)

        m = S3Manifest.from_bucket(empty_bucket, "/")
        self.assertEqual(len(m.entries), 0)

    def skip_test_from_bucket__lotsafiles(self):
        '''This test is intended to test the continuation code.

        Unfortunately, it doesn't seem like moto ever sends these.
        '''
        nonempty_bucket = "somanyfiles"

        N = 1000

        self._build_s3(nonempty_bucket, [[f'/{i}', f'{i}', ""] for i in range(N)])

        m = S3Manifest.from_bucket(nonempty_bucket, "/")
        self.assertEqual(len(m.entries), N)
