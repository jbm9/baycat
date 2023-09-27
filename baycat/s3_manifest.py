from collections import defaultdict
from io import BytesIO
import json
import logging
import os

from .s3_file import S3File
from .json_serdes import JSONSerDes, baycat_json_decoder, BaycatJSONEncoder
from .manifest import Manifest

import boto3
from botocore.exceptions import ClientError

S3MANIFEST_FILENAME = ".baycat_s3manifest"


class S3Manifest(Manifest):
    JSON_CLASSNAME = "S3Manifest"

    def __init__(self, path=S3MANIFEST_FILENAME, bucket_name=None, root=None):
        self.bucket_name = bucket_name
        self.root = root
        self.entries = {}  # path => S3File
        self.path = path
        self.counters = defaultdict(int)

        self.root_path = os.path.join(bucket_name, root)

        self.s3 = boto3.client("s3")

    def expand_path(self, rel_path):
        return os.path.join(self.root_path, rel_path)

    def upload_file(self, src_path, dst_path):
        self.s3.upload_file(src_path, self.bucket_name, dst_path)
        self.counters["s3_uploads"] += 1

    def download_file(self, src_path, dst_path):
        self.s3.download_file(self.bucket_name, src_path, dst_path)
        self.counters["s3_downloads"] += 1

    def save(self, path=None, overwrite=True):
        if path is None:
            path = os.path.join(self.root_path, self.path)

        if not overwrite:
            raise ValueError('Request to not overwrite a manifest in S3, but I am lazy.')

        fh = BytesIO(json.dumps(self, default=BaycatJSONEncoder().default).encode('utf8'))
        self.counters["bytes_up"] += len(fh.getvalue())
        self.s3.upload_fileobj(fh, self.bucket_name, path)
        self.counters["s3_uploads"] += 1
        logging.debug('Saved to s3://%s/%s' % (self.bucket_name, path))

    @classmethod
    def load(cls, bucket_name, root):
        if False:
            uri = urlparse(path)
            if uri is None or uri.scheme != "s3":
                raise ValueError(f'Could parse "{path} as S3 URI.  Expected s3://bucket/root/path')
            bucket_name = uri.netloc
            root = uri.path

        full_path = os.path.join(root, S3MANIFEST_FILENAME)

        logging.debug('Load from s3://%s%s' % (bucket_name, full_path))

        fh = BytesIO()
        try:
            s3 = boto3.client("s3")
            s3.download_fileobj(bucket_name, full_path, fh)
        except ClientError as e:
            logging.error(f'Error fetching bucket contents: {e}')
            raise e

        result = cls.from_json_obj(json.loads(fh.getvalue().decode('utf8')))
        return result

    def update(self):
        '''Note that we lose a bunch of metadata in S3, so update is half-meaningless.

        However, it does let us find any file contents were are
        already in the target location on S3.  This allows us to
        "adopt" existing uploads from other tooling, as well as
        recover from partial uploads that failed to update the s3
        manifest file.
        '''
        response = self._fetch_objs(self.bucket_name, self.root)

        if response['KeyCount'] == 0:
            return

        for objsum in response['Contents']:
            self._add_entry(objsum)

        while response['IsTruncated']:
            response = self._fetch_objs(bucket_name, root, response['NextContinuationToken'])
            for objsum in response['Contents']:
                self._add_entry(objsum)

    def to_json_obj(self):
        result = {
            "_json_classname": self.JSON_CLASSNAME,
            "root": self.root,
            "bucket_name": self.bucket_name,
            "entries": {}
        }

        for p, f in self.entries.items():
            result["entries"][p] = f.to_json_obj()

        return result

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            logging.debug(f'Got a non-JSON-SerDes object in Manifest!')
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in Manifest!')
            raise ValueError(f'Got invalid object!')

        result = S3Manifest(bucket_name=obj["bucket_name"], root=obj["root"])
        result.entries = obj["entries"]
        for k, v in result.entries.items():
            result.entries[k] = baycat_json_decoder(v)
        return result

    def _add_entry(self, objsum):
        s3f = S3File.from_objsum(self.root_path, objsum)
        dpath = s3f.rel_path

        filename = os.path.basename(dpath)
        filedir = os.path.dirname(dpath)
        if filedir == self.root and filename == S3MANIFEST_FILENAME:
            logging.debug('Skipping _add_entry for "%s", as it looks like my manifest' % objsum["Key"])
            return

        self.entries[dpath] = s3f

    def _fetch_objs(self, bucket_name, root, token=None):
        kwargs = {}
        if token:
            kwargs["ContinuationToken"] = token

        try:
            s3 = boto3.client("s3")
            response = s3.list_objects_v2(Bucket=bucket_name,
                                          Prefix=root,
                                          **kwargs)
            self.counters["s3_list_objects"] += 1
            return response
        except ClientError as e:
            logging.error(f'Error fetching bucket contents: {e}')
            raise e

    @classmethod
    def from_bucket(cls, bucket_name, root="/"):
        result = S3Manifest(bucket_name=bucket_name, root=root)
        result.update()

        return result
