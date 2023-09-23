import datetime
from io import BytesIO
import logging
import os
import shutil
import sys
import tempfile
import unittest

import boto3
from moto import mock_s3

import baycat
from baycat.local_file import LocalFile
from baycat.s3_file import S3File


class BaycatTestCase(unittest.TestCase):
    '''Base class for all baycat test cases

    This is a pretty straightforward set of helper methods that create
    a fresh directory for unit tests to work against, and manipulate
    at will.
    '''
    # A set of directories to create.  You can bum these down to just
    # the leaf directories in the structure, to simplify life a bit.
    # That is, 'a/b/c/d' will create 'a/', then 'a/b/', then 'a/b/c/',
    # and finally the last directory, 'a/b/c/d/'.
    #
    # Note that some of these should be empty, to test our creation of
    # empty directories.  Ideally, we will have a small chain of empty
    # subdirs, to catch any errors in descending into those.
    LEAF_DIRS = ["a/b/c/d"]

    # These are the dummy files and their contents that we set up
    # every run.
    FILECONTENTS = [
            ("a/afile", "contents of afile", "79c36f925735a81867048aa3c3a87b93"),
            ("a/afile2", "more contents", "fc94a20a012e5014fc2ea79b4efcb97f"),
            ("a/b/bfile", "some content", "9893532233caff98cd083a116b013c0b"),
            ("a/b/bfile2", "you're never gonna guess", "31aeb6de5b580dc89d3e101260eccd87"),
            ("a/b/bfile3",
             "This is fine \U0001f525\U0001f525\U0001f436\u2615\ufe0f\U0001f525\U0001f525",
             "5cb7a8a7b77a0bdedc3f1a5ee7392743"),
        ]

    # Name of our testing S3 bucket
    BUCKET_NAME = "mah_bukkit"
    S3_PATH = "/oh/no/"

    def _build_dirs(self, base_suffix, leaf_dirs, file_contents):
        dpath = os.path.join(self.base_dir, base_suffix)
        os.makedirs(dpath, exist_ok=True)

        for dirpath in leaf_dirs:
            os.makedirs(os.path.join(dpath, dirpath), exist_ok=True)

        for subpath, content, _ in file_contents:
            path = os.path.join(dpath, subpath)
            with open(path, "w+") as f:
                f.write(content)

        return dpath

    def _mk_bucket(self, bucket_name):
        s3 = boto3.resource("s3")
        bucket = s3.create_bucket(Bucket=bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ur-butt-1'})
        return bucket

    def _build_s3(self, bucket_name, file_contents):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Set up our S3
        bucket = self._mk_bucket(bucket_name)

        for subpath, content, _ in file_contents:
            dstpath = os.path.join(self.S3_PATH, subpath)
            fh = BytesIO(content.encode("UTF8"))
            bucket.upload_fileobj(fh, dstpath)

    def setUp(self):
        '''Creates a temporary test directory, and populates it with the
        directories and files as expected.
        '''

        if 'BAYCAT_TEST_LOGLEVEL' in os.environ:
            log_level = os.environ['BAYCAT_TEST_LOGLEVEL']
            logging.basicConfig(level=log_level.upper())
        else:
            # Let's turn off logging during tests by default...
            logging.disable(logging.CRITICAL)

        # The following are Chatty Cathies.
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("s3transfer").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)

        self.base_dir = tempfile.mkdtemp(prefix="baycat_unit_test-")

        self.test_dir = self._build_dirs("0", self.LEAF_DIRS, self.FILECONTENTS)

        self._build_s3(self.BUCKET_NAME, self.FILECONTENTS)

    def tearDown(self):
        '''Currently just nukes the tempdir we created in setUp'''
        self.mock_s3.stop()
        shutil.rmtree(self.base_dir)

    def _get_lf(self, subpath=None, do_checksum=False):
        # Get a LocalFile for the given path, or the default if none given
        if subpath is None:
            subpath = self.FILECONTENTS[0][0]

        return LocalFile.for_path(self.test_dir, subpath, do_checksum=do_checksum)

    def _get_s3f(self, root_path="/lol/", objsum=None, key=None):
        if objsum is None:
            objsum = {
                "Key": os.path.join(root_path, self.FILECONTENTS[0][0]) if key is None else key,
                "ETag": self.FILECONTENTS[0][2],
                "Size": len(self.FILECONTENTS[0][1]),
                "LastModified": datetime.datetime(2022, 3, 4),
            }
        return S3File(root_path, objsum)

    def _ith_path(self, i, tgt_dir=None, fileset=None):
        # Get the full path to the i'th test file

        tgt_dir = tgt_dir or self.test_dir
        fileset = fileset or self.FILECONTENTS

        return os.path.join(tgt_dir, fileset[i][0])
