import json
import os
import unittest
import shutil
import tempfile

from context import baycat

from baycat.file_selectors import PathSelector
from baycat.local_file import LocalFile, ReservedNameException
from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestFileSelectors(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _get_lf(self, path="/etc/passwd"):
        return LocalFile.for_path(path)

    def test_walk__happy_path(self):
        os.makedirs(os.path.join(self.test_dir, "a/b/c/d"), exist_ok=True)

        files = [
            ("a/afile", "contents of afile"),
            ("a/afile2", "more contents"),
            ("a/b/bfile", "some content"),
            ("a/b/bfile2", "you're never gonna guess"),
        ]

        for subpath, content in files:
            path = os.path.join(self.test_dir, subpath)
            with open(path, "w+") as f:
                f.write(content)

        expected_dirs = ["", "a/", "a/b/", "a/b/c/", "a/b/c/d/",]

        expected_paths = []
        expected_paths += [ f'{s}.baycat_dir_metadata' for s in expected_dirs ]
        expected_paths += [ f'{p}' for p, _ in files ]

        expected_paths = [ os.path.join(self.test_dir, p) for p in expected_paths ]

        got_paths = []

        ps = PathSelector(self.test_dir)

        for lf in ps.walk():
            got_paths.append(lf.path)

        self.assertEqual(sorted(got_paths), sorted(expected_paths))

    def test_walk__reservedname(self):
        os.makedirs(os.path.join(self.test_dir, "a/b/c/d"), exist_ok=True)

        files = [
            ("a/afile", "contents of afile"),
            ("a/afile2", "more contents"),
            ("a/b/bfile", "some content"),
            ("a/b/bfile2", "you're never gonna guess"),
        ]

        for subpath, content in files:
            path = os.path.join(self.test_dir, subpath)
            with open(path, "w+") as f:
                f.write(content)

        with open(os.path.join(self.test_dir, "a/b/.baycat_dir_metadata"), "w+") as f:
            f.write("foo")

        expected_dirs = ["", "a/", "a/b/", "a/b/c/", "a/b/c/d/",]

        expected_paths = []
        expected_paths += [ f'{s}.baycat_dir_metadata' for s in expected_dirs ]
        expected_paths += [ f'{p}' for p, _ in files ]

        expected_paths = [ os.path.join(self.test_dir, p) for p in expected_paths ]

        got_paths = []

        ps = PathSelector(self.test_dir)

        for lf in ps.walk():
            got_paths.append(lf.path)

        self.assertEqual(sorted(got_paths), sorted(expected_paths))


if __name__ == '__main__':
    unittest.main()
