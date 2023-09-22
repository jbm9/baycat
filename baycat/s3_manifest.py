from io import BytesIO
import logging
import os

from .s3_file import S3File
from .json_serdes import JSONSerDes, baycat_json_decoder

import boto3
from botocore.exceptions import ClientError

S3MANIFEST_FILENAME = ".s3baycat_manifest"


class S3Manifest(JSONSerDes):
    JSON_CLASSNAME = "S3Manifest"

    def __init__(self, path=S3MANIFEST_FILENAME, bucket_name=None, prefix=None):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.entries = {}  # path => S3File
        self.path = path

        self.root_path = os.path.join(bucket_name, prefix)

        self.s3 = boto3.client("s3")

    def _expand_path(self, rel_path):
        return os.path.join(self.root_path, rel_path)

    def upload_file(self, src_path, dst_path):
        self.s3.upload_file(src_path, self.bucket_name, dst_path)

    def save(self, path=None, overwrite=True):
        if path is None:
            path = os.path.join(self.root_path, self.path)

        if not overwrite:
            raise ValueError('Request to not overwrite a manifest in S3, but I am lazy.')

        fh = BytesIO(json.dumps(self,f, default=BaycatJSONEncoder().default))
        self.s3.upload_fileobj(fh, self.bucket_name, path)

    def to_json_obj(self):
        result = {
            "_json_classname": self.JSON_CLASSNAME,
            "prefix": self.prefix,
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
            logging.debug(f'Got a value of class {obj["_json_classname"]} in Manifeste!')
            raise ValueError(f'Got invalid object!')

        result = S3Manifest(bucket_name=obj["bucket_name"], prefix=obj["prefix"])
        result.entries = obj["entries"]
        for k, v in result.entries.items():
            if v.__class__ == dict:
                result.entries[k] = baycat_json_decoder(v)
        return result

    def _add_entry(self, objsum):
        s3f = S3File(self.root_path, objsum)
        dpath = s3f.rel_path
        self.entries[dpath] = s3f

    @classmethod
    def _fetch_objs(cls, bucket_name, prefix, token=None):

        kwargs = {}
        if token:
            kwargs["ContinuationToken"] = token

        try:
            s3 = boto3.client("s3")
            response = s3.list_objects_v2(Bucket=bucket_name,
                                          Prefix=prefix,
                                          **kwargs)
            return response
        except ClientError as e:
            logging.error(f'Error fetching bucket contents: {e}')
            raise e

    @classmethod
    def from_bucket(cls, bucket_name, prefix="/"):

        result = S3Manifest(bucket_name=bucket_name, prefix=prefix)

        response = cls._fetch_objs(bucket_name, prefix)

        if response['KeyCount'] == 0:
            return result

        for objsum in response['Contents']:
            result._add_entry(objsum)

        while response['IsTruncated']:
            response = cls._fetch_objs(bucket_name, prefix, response['NextContinuationToken'])
            for objsum in response['Contents']:
                result._add_entry(objsum)

        return result

    def __eq__(self, m_b):
        if self.__class__ != m_b.__class__:
            return False

        # self.path is not checked here, as we can store the same
        # manifest in multiple locations.

        # Similarly, self.root is not checked

        if len(self.entries) != len(m_b.entries):
            return False

        all_paths = set(self.entries.keys()) | set(m_b.entries.keys())

        if len(all_paths) != len(self.entries):
            return False

        for p in all_paths:
            lf_s = self.entries[p]
            lf_b = m_b.entries[p]

            if lf_s != lf_b:
                return False

        return True

    def mark_mkdir(self, rel_p):
        '''Callback of sorts for a successful mkdir call; not needed in S3'''
        pass
