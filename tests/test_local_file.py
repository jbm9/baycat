import json
import os
import shutil
import tempfile
import unittest

from context import baycat

from baycat.local_file import LocalFile, ChecksumMissingException, \
    ChecksumKindException, PathMismatchException, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestLocalFile(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _get_lf(self, path="/etc/passwd"):
        return LocalFile.for_path(path)

    def test__reservednameexception(self):
        path = os.path.join(self.test_dir, ".baycat_dir_metadata")
        f = open(path, "w+")
        f.write("hi moM")
        f.close()

        self.assertRaises(ReservedNameException,
                          lambda: LocalFile.for_path(path))

    def test_serdes__happy_path(self):
        lf = self._get_lf()

        js_version = lf.to_json_obj()

        round_trip = LocalFile.from_json_obj(js_version)

        self.assertEqual(round_trip, lf)

    def test_serdes__eq(self):
        lf = self._get_lf()
        lf2 = self._get_lf()

        # We have to fudge this here
        lf2.collected = lf.collected

        self.assertEqual(lf, lf2)

        lf2.cksum_type = "SHA-512"

        self.assertNotEqual(lf, lf2)

        self.assertNotEqual(lf, True)
        self.assertNotEqual(lf, None)
        self.assertNotEqual(lf, 42)

    def test_serdes__bogon(self):
        self.assertRaises(ValueError, lambda: LocalFile.from_json_obj({"hi": "mom"}))
        self.assertRaises(ValueError, lambda: LocalFile.from_json_obj({"_json_classname": "Manifest"}))

    def test_serdes__with_json_lib(self):
        lf = self._get_lf()

        json_body = json.dumps(lf, default=BaycatJSONEncoder.default)
        d = json.loads(json_body)
        round_trip = json.loads(json_body, object_hook=baycat_json_decoder)

        self.assertEqual(lf, round_trip)

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

        lf2 = self._get_lf("/etc")
        self.assertRaises(PathMismatchException,
                          lambda: lf.delta(lf2))


if __name__ == '__main__':
    unittest.main()
