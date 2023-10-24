import logging
import json
import os
import unittest
import shutil
import tempfile
import time

from context import baycat, BaycatTestCase

from baycat.json_serdes import BaycatJSONEncoder, baycat_json_decoder


class TestJSONSerDes(BaycatTestCase):
    def test_baycat_json_decoder__missing_cls(self):
        dummy = { "_json_classname": "Jason" }

        self.assertRaises(ValueError, lambda: baycat_json_decoder(dummy))
