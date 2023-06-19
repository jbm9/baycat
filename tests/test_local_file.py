import json
import os
import shutil
import tempfile
import time
import unittest

from context import baycat, BaycatTestCase

from baycat.local_file import LocalFile, ChecksumMissingException, \
    ChecksumKindException, PathMismatchException, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestLocalFile(BaycatTestCase):
    def test__reservednameexception(self):
        rel_path = ".baycat_dir_metadata"
        path = os.path.join(self.test_dir, rel_path)
        f = open(path, "w+")
        f.write("hi moM")
        f.close()

        self.assertRaises(ReservedNameException,
                          lambda: LocalFile.for_path(self.test_dir, rel_path))

    def test_str__doesnt_explode(self):
        # This is a stupid smoke test
        lf = self._get_lf()
        s = str(lf)
        self.assertTrue(True)

    def test_copy(self):
        lf = self._get_lf()
        lf_copy = lf.copy()

        self.assertEqual(lf, lf_copy)

        lf_copy.rel_path = "/your/mom"
        self.assertNotEqual(lf, lf_copy)

    def test_for_path__happypath(self):
        # NB: this does double-duty: it tests for_path very gently,
        # but it also lets us smoke test the filesystem population
        # helpers in BaycatTestCase.

        for subpath, contents, exp_hash in BaycatTestCase.FILECONTENTS:
            path = os.path.join(self.test_dir, subpath)
            lf = LocalFile.for_path(self.test_dir, subpath, do_checksum=True)
            self.assertEqual(path, lf.path, path)
            self.assertEqual(len(contents.encode("UTF-8")), lf.size, path)
            self.assertEqual(exp_hash, lf.cksum, path)

    def test_for_abspath(self):
        for subpath, contents, exp_hash in BaycatTestCase.FILECONTENTS:
            path = os.path.join(self.test_dir, subpath)
            lf = LocalFile.for_abspath(self.test_dir, path, do_checksum=True)
            self.assertEqual(path, lf.path, path)
            self.assertEqual(len(contents.encode("UTF-8")), lf.size, path)
            self.assertEqual(exp_hash, lf.cksum, path)

    def test_for_abspath__misrooted(self):
        self.assertRaises(PathMismatchException, lambda: LocalFile.for_abspath(self.test_dir, '/etc/passwd'))

    def test_for_abspath__with_slash(self):
        for subpath, contents, exp_hash in BaycatTestCase.FILECONTENTS:
            path = os.path.join(self.test_dir, subpath)
            lf = LocalFile.for_abspath(self.test_dir+"/", path, do_checksum=True)
            self.assertEqual(path, lf.path, path)
            self.assertEqual(len(contents.encode("UTF-8")), lf.size, path)
            self.assertEqual(exp_hash, lf.cksum, path)

    def test_serdes__happy_path(self):
        lf = self._get_lf()

        js_version = lf.to_json_obj()

        round_trip = LocalFile.from_json_obj(js_version)

        self.assertEqual(round_trip, lf)

    def test_serdes__bogon(self):
        self.assertRaises(ValueError, lambda: LocalFile.from_json_obj({"hi": "mom"}))
        self.assertRaises(ValueError, lambda: LocalFile.from_json_obj({"_json_classname": "Manifest"}))

    def test_serdes__with_json_lib(self):
        lf = self._get_lf()

        json_body = json.dumps(lf, default=BaycatJSONEncoder.default)
        d = json.loads(json_body)
        round_trip = json.loads(json_body, object_hook=baycat_json_decoder)

        self.assertEqual(lf, round_trip)

    def test_eq(self):
        lf = self._get_lf()
        lf2 = self._get_lf()

        self.assertEqual(lf, lf2)

        lf2.cksum_type = "SHA-512"

        self.assertNotEqual(lf, lf2)

        self.assertNotEqual(lf, True)
        self.assertNotEqual(lf, None)
        self.assertNotEqual(lf, 42)

    def test_delta__happy_path(self):
        lf = self._get_lf()
        lf2 = self._get_lf()

        delta = lf.delta(lf2)

        self.assertFalse(delta["_dirty"])

    def test_delta__exceptions(self):
        lf = self._get_lf()
        lf2 = self._get_lf()

        self.assertRaises(ChecksumMissingException,
                          lambda: lf.delta(lf2, compare_checksums=True))

        lf2 = self._get_lf()
        lf2.cksum_type = "trustmesum"
        self.assertRaises(ChecksumKindException,
                          lambda: lf.delta(lf2, compare_checksums=False))

        lf2 = self._get_lf("a/afile2")
        self.assertRaises(PathMismatchException,
                          lambda: lf.delta(lf2))

    def test_delta__fields(self):
        # this is gonna hurt me more than it hurts you
        def _assertDirty(lf_p, fields, **kwargs):
            delta = lf_p.delta(lf, **kwargs)

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

        lf = self._get_lf(do_checksum=True)

        # Quick first gut check that we're starting clean
        lf2 = self._get_lf(do_checksum=True)
        _assertDirty(lf2, [])

        # And now to walk through fields
        lf2 = self._get_lf()
        lf2.size += 1
        _assertDirty(lf2, ["size"])

        lf2 = self._get_lf()
        lf2.mtime_ns += 1
        _assertDirty(lf2, ["mtime_ns"])

        lf2 = self._get_lf()
        lf2.cksum = '0123456'
        _assertDirty(lf2, ["cksum"], compare_checksums=True)

        # And make sure force recomputing the checksum does the right thing
        lf2 = self._get_lf()
        lf2.cksum = '0123456'
        _assertDirty(lf2, ['_recomputed_cksum'], compare_checksums=True, force_checksum=True)

        lf2 = self._get_lf()
        lf2.cksum = None
        _assertDirty(lf2, ['_recomputed_cksum'], compare_checksums=True, force_checksum=False)

        lf2 = self._get_lf()
        lf2.size += 1
        lf2.mtime_ns += 1
        _assertDirty(lf2, ["mtime_ns", "size"])

        for k in lf.metadata.keys():
            lf2 = self._get_lf()
            lf2.metadata[k] += 1  # These are all numbers currently
            if k != "atime_ns":
                _assertDirty(lf2, [k])
            else:
                _assertDirty(lf2, [])

    def test_changed_from(self):
        lf = self._get_lf()
        lf2 = self._get_lf()

        self.assertEqual(lf, lf2)
        self.assertFalse( lf.changed_from(lf2) )

        # Change mtime and size
        p = os.path.join(self.test_dir, self.FILECONTENTS[0][0])
        with open(p, "w") as fh:
            fh.write("This is different content")

        lf3 = self._get_lf(do_checksum=True)
        self.assertTrue(lf.changed_from(lf3))

        # Now change mtime with different size
        with open(p, "w") as fh:
            fh.write("This is different cont123")
        time.sleep(0.01)

        lf4 = self._get_lf(do_checksum=False)

        self.assertTrue(lf.changed_from(lf4))
        self.assertTrue(lf3.changed_from(lf4))

        # Let's transfer the metadata, and only compare checksums
        os.utime(p, ns=lf3.get_utime())
        time.sleep(0.01)
        lf5 = self._get_lf()

        # Should be equivalent unless we force checksums
        self.assertFalse(lf5.changed_from(lf3))
        # And then we notice the difference
        self.assertTrue(lf5.changed_from(lf3, force_checksum=True))


