import json
import os
import shutil
import tempfile
import time
import unittest

from context import baycat, BaycatTestCase

from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder
from baycat.local_file import ChecksumKindException, PathMismatchException, ReservedNameException
from baycat.s3_file import S3File


class TestS3File(BaycatTestCase):
    def test_str__doesnt_explode(self):
        # This is a stupid smoke test
        s3f = self._get_s3f()
        s = str(s3f)
        self.assertTrue(True)

    def test_copy(self):
        s3f = self._get_s3f()
        s3f_copy = s3f.copy()

        self.assertEqual(s3f, s3f_copy)

        s3f_copy.rel_path = "/your/mom"
        self.assertNotEqual(s3f, s3f_copy)

    def test_serdes__happy_path(self):
        s3f = self._get_s3f()

        js_version = s3f.to_json_obj()

        round_trip = S3File.from_json_obj(js_version)
        self.assertEqual(round_trip, s3f)

    def test_serdes__bogon(self):
        self.assertRaises(ValueError, lambda: S3File.from_json_obj({"hi": "mom"}))
        self.assertRaises(ValueError, lambda: S3File.from_json_obj({"_json_classname": "Manifest"}))

    def test_serdes__with_json_lib(self):
        s3f = self._get_s3f()

        json_body = json.dumps(s3f, default=BaycatJSONEncoder().default)
        d = json.loads(json_body)
        round_trip = json.loads(json_body, object_hook=baycat_json_decoder)

        self.assertEqual(s3f, round_trip)

    def test_eq(self):
        s3f = self._get_s3f()
        s3f2 = self._get_s3f()

        self.assertEqual(s3f, s3f2)

        s3f2.cksum_type = "SHA-512"

        self.assertNotEqual(s3f, s3f2)

        self.assertNotEqual(s3f, True)
        self.assertNotEqual(s3f, None)
        self.assertNotEqual(s3f, 42)

    def test_delta__happy_path(self):
        s3f = self._get_s3f()
        s3f2 = self._get_s3f()

        delta = s3f.delta(s3f2)

        self.assertFalse(delta["_dirty"])

    def test_delta__exceptions(self):
        s3f = self._get_s3f()
        s3f2 = self._get_s3f()

        s3f2.cksum_type = "trustmesum"
        self.assertRaises(ChecksumKindException,
                          lambda: s3f.delta(s3f2, compare_checksums=False))

        s3f2 = self._get_s3f(key="a/afile2")
        self.assertRaises(PathMismatchException,
                          lambda: s3f.delta(s3f2))

    def test_delta__fields(self):
        # this is gonna hurt me more than it hurts you
        def _assertDirty(s3f_p, fields, **kwargs):
            delta = s3f_p.delta(s3f, **kwargs)

            mismatched_fields = []
            for k, v in delta.items():
                is_mismatch = (v != (k in fields))
                if k == "_dirty":
                    is_mismatch = (v != bool(fields))
                elif k == "cksum" and v is None:
                    is_mismatch = False

                if is_mismatch:
                    mismatched_fields.append(f'{k}: {v}')

            self.assertCountEqual([], mismatched_fields)

        s3f = self._get_s3f()

        # Quick first gut check that we're starting clean
        s3f2 = self._get_s3f()
        _assertDirty(s3f2, [])

        # And now to walk through fields
        s3f2 = self._get_s3f()
        s3f2.size += 1
        _assertDirty(s3f2, ["size"])

        s3f2 = self._get_s3f()
        s3f2.mtime_ns += 1.1e9
        _assertDirty(s3f2, ["mtime_ns"])

        s3f2 = self._get_s3f()
        s3f2.cksum = '0123456'
        _assertDirty(s3f2, ["cksum"], compare_checksums=True)

        s3f2 = self._get_s3f()
        s3f2.size += 1
        s3f2.mtime_ns += 1.1e9
        _assertDirty(s3f2, ["mtime_ns", "size"])

        for k in s3f.metadata.keys():
            s3f2 = self._get_s3f()
            if s3f2.metadata[k] is None:
                continue  # Weird artifacts left over XXX TODO
            s3f2.metadata[k] += 1  # These are all numbers currently
            if k != "atime_ns":
                _assertDirty(s3f2, [k])
            else:
                _assertDirty(s3f2, [])
