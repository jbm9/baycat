import logging
import os

from .manifest import Manifest, MANIFEST_FILENAME
from .s3_manifest import S3Manifest, S3MANIFEST_FILENAME, ClientError
from .sync_strategies import SyncLocalToLocal, SyncLocalToS3, SyncS3ToLocal


class CLIImpl:
    def __init__(self):
        self.verbose = False
        self.dry_run = False

    def path_is_s3(self, p):
        return p.startswith("s3://")

    def _load_manifest(self, path):
        is_s3 = self.path_is_s3(path)

        if not is_s3:
            try:
                m = Manifest.load(path)
            except FileNotFoundError as e:
                logging.debug("Didn't find manifest for %s, creating a new one" % (path,))
                m = Manifest.for_path(path)
        else:
            m = S3Manifest.from_uri(path)
            # Don't handle any errors here.
        return m

    def sync(self, path_src, path_dst):
        logging.debug('Syncing from %s to %s' % (path_src, path_dst))
        src_s3 = self.path_is_s3(path_src)
        dst_s3 = self.path_is_s3(path_dst)

        if src_s3 and dst_s3:
            raise ValueError(f"S3-to-S3 not supported")

        if not dst_s3:
            # destination is local, need to ensure it exists
            if not os.path.isdir(path_dst):
                if os.path.exists(path_dst):
                    raise FileExistsError(f'Destination {path_dst} exists, but is not a dir, so cannot sync')
                if self.dry_run:
                    raise FileNotFoundError(f'Destination {path_dst} does not exist, so cannot create a manifest')
                os.makedirs(path_dst)

        # Load our manifests from the wire
        m_src = self._load_manifest(path_src)
        m_dst = self._load_manifest(path_dst)

        # Update the source manifest
        m_src.update()
        if not self.dry_run and not src_s3:
            m_src.save(overwrite=True)

        sync_opts = {
            'dry_run': self.dry_run,
            'verbose': self.verbose,
        }

        ss = None
        if not src_s3 and not dst_s3:
            ss = SyncLocalToLocal(m_src, m_dst, **sync_opts)
        elif not src_s3 and dst_s3:
            ss = SyncLocalToS3(m_src, m_dst, **sync_opts)
        elif src_s3 and not dst_s3:
            ss = SyncS3ToLocal(m_src, m_dst, **sync_opts)

        m_xfer = ss.sync()
        m_xfer.save(overwrite=True)
        return ss
