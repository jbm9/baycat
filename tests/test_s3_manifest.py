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
    def test__entries_keys(self):
        '''Ensure that the rel_path keys are as expected'''
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)

        keys_got = set(m.entries.keys())
        keys_expected = set([a[0] for a in self.FILECONTENTS])

        # Note that S3 doesn't have directories, so we only expect
        # files here

        self.assertEqual(keys_got, keys_expected)

    def test_from_bucket__happypath(self):
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)
        self.assertEqual(len(m.entries), len(self.FILECONTENTS))

        for relpath, contents, md5sum in self.FILECONTENTS:
            s3f = m.entries[relpath]
            self.assertEqual(str(md5sum), s3f.cksum)

        # And check out from_uri while we're at it.
        m.save()  # Get our manifest up there
        s3_uri = f's3://{self.BUCKET_NAME}/{self.S3_PATH}'
        m = S3Manifest.from_uri(s3_uri)
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
                          lambda: S3Manifest.from_bucket("yourmom"))

        self.assertRaises(ClientError,
                          lambda: S3Manifest.from_uri("s3://yourmom"))

    def test_from_bucket__empty(self):
        empty_bucket = "empty"
        self._mk_bucket(empty_bucket)

        m = S3Manifest.from_bucket(empty_bucket)
        self.assertEqual(len(m.entries), 0)

    def test_from_bucket__lotsafiles(self):
        '''This test is intended to test the continuation code.
        '''
        nonempty_bucket = "somanyfiles"

        N = 1001

        self._build_s3(nonempty_bucket, [[f'{i}', f'{i}', ""] for i in range(N)])

        m = S3Manifest.from_bucket(nonempty_bucket)
        self.assertEqual(len(m.entries), N)

    def test_load__exceptions(self):
        self.assertRaises(ClientError, lambda: S3Manifest.load('nonbucket', '/foo'))

    def test_save_and_load(self):
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)

        m.save()

        m_got = S3Manifest.load(self.BUCKET_NAME, self.S3_PATH)

        self.assertEqual(m, m_got)

    def test_save__overwrite(self):
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)
        self.assertRaises(ValueError, lambda: m.save(overwrite=False))

    def test_from_json_obj__bogons(self):
        self.assertRaises(ValueError, lambda: S3Manifest.from_json_obj({}))
        m = S3Manifest.from_bucket(self.BUCKET_NAME, self.S3_PATH)

        s3f = m.entries[self.FILECONTENTS[0][0]]
        self.assertRaises(ValueError, lambda: S3Manifest.from_json_obj(s3f.to_json_obj()))

    def test_expand_path(self):
        m = S3Manifest('_', self.BUCKET_NAME, '/root/path/foo')

        self.assertEqual(m.expand_path('/another/path'),
                         'root/path/foo/another/path')
