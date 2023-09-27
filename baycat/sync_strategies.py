from abc import ABC, abstractmethod
from collections import defaultdict
import logging
import os
import os.path
import pathlib
import shutil
import tempfile

import boto3
from botocore.exceptions import ClientError


class SyncStrategy(ABC):
    def __init__(self, manifest_src, manifest_dst, enable_delete=False, dry_run=False, verbose=False):
        self.manifest_src = manifest_src
        self.manifest_dst = manifest_dst
        self.manifest_xfer = manifest_dst.copy()
        logging.debug(f'Sync: "{manifest_src.root}" -> "{manifest_dst.root}"')

        self.enable_delete = enable_delete
        self.dry_run = dry_run
        self.verbose = verbose

        if self.dry_run and self.enable_delete:
            self.enable_delete = False

        self._counters = defaultdict(int)

    def sync(self, dry_run=False):
        self.diff_state = self.manifest_src.diff_from(self.manifest_dst)

        # Maintaining consistent directory metadata is a real pain.
        # Every time we manipulate a file's contents (or even its
        # metadata), we modify its parent directory's mtime.  And
        # then, when we try to fix up that mtime, we upset *its*
        # parent's mtime.  So, we have to keep track of every path we
        # touch, and then turn that into a set of paths all the way to
        # the root of the tree.  We then re-apply metadata from the
        # source version of each of these directories, going strictly
        # bottom-up.

        paths_touched = []  # Files we've touched
        dir_fixups = set()  # A set of (depth, dirpath) tuples

        for rel_p in self.diff_state["deleted"]:
            self.rm_file(rel_p)
            paths_touched.append(rel_p)

        # Create all the directories so our files have homes
        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if not lf.is_dir:
                continue
            self.mkdir(rel_p)

            # Add the dir to the list of "files" touched
            paths_touched.append(rel_p)

            # And mark it explicitly as a directory to fixup
            n_comps = len(rel_p.split("/"))
            dir_fixups.add((-n_comps, rel_p))

        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if lf.is_dir:
                continue

            self.transfer_file(rel_p)
            self.transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["contents"]:
            self.transfer_file(rel_p)
            self.transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["metadata"]:
            self.transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        # Now we compute the order in which to fix up metadata for
        # directories.  First, we add the root if needed.
        if paths_touched:
            paths_touched.append(".")

        for rel_p in paths_touched:
            p_cur = rel_p
            while p_cur and p_cur != "/":
                p_cur = os.path.dirname(p_cur)
                n_comps = len(p_cur.split("/"))
                if p_cur and p_cur != "/":
                    dir_fixups.add((-n_comps, p_cur))

        # TODO Place the manifest file in the destination

        # And finally play back all the metadata, in order of
        # decreasing depth.
        for _, d in sorted(list(dir_fixups)):
            if d not in self.diff_state["deleted"]:
                self.transfer_metadata(d)

        return self.manifest_xfer

    def rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        logging.debug('%s.rm_file(%s)' % (type(self).__name__, rel_p))
        if self.enable_delete:
            self._counters["rm"] += 1
            try:
                if self.verbose:
                    logging.info(f'Removing file {rel_p}')

                self._rm_file(rel_p)
            except Execption as e:
                logging.error(f'Error while deleting {rel_p}: {e}')
                return
        else:
            self._counters["delete_skipped"] += 1

            if self.dry_run:
                logging.info(f"dry_run: remove {rel_p}")

        self.manifest_xfer.mark_deleted(rel_p)

    @abstractmethod
    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''

    def transfer_file(self, rel_p):
        logging.debug('%s.transfer_file(%s)' % (str(type(self).__name__), rel_p))
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        src_lf = self.manifest_src.entries[rel_p]
        if not src_lf.is_dir and not self.dry_run:
            self._counters["xfer"] += 1
            try:
                if self.verbose:
                    logging.info(f'Transfer file {rel_p}')

                self._transfer_file(rel_p)
            except Exception as e:
                logging.error(f'Error while transferring {rel_p}: {e}')
                # Don't mark as transferred, because it wasn't.
                return
        elif self.dry_run:
            logging.info(f"dry_run: transfer {src_path} -> {dst_path}")

        self.manifest_xfer.mark_transferred(rel_p, src_lf)

    @abstractmethod
    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''

    def transfer_metadata(self, rel_p):
        logging.debug('%s.transfer_metadata (%s)' % (type(self).__name__, rel_p))
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        if not self.dry_run:
            self._counters["xfer_metadata"] += 1
            try:
                if self.verbose:
                    logging.info(f'Transfer metadata {rel_p}')

                self._transfer_metadata(rel_p)
            except Exception as e:
                logging.error(f'Error while transferring metadata for {rel_p}: {e}')
                return
        else:
            logging.info(f"dry_run: transfer metadata {src_path} -> {dst_path}")

        src_lf = self.manifest_src.entries[rel_p]
        self.manifest_xfer.mark_transferred(rel_p, src_lf)

    @abstractmethod
    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''

    def mkdir(self, rel_p):
        logging.debug('%s.mkdir (%s)' % (type(self).__name__, rel_p))
        dst_path = self.manifest_dst._expand_path(rel_p)

        if not self.dry_run:
            self._counters["mkdir"] += 1
            try:
                if self.verbose:
                    logging.info(f'mkdir {rel_p}')

                self._mkdir(rel_p)
            except Exception as e:
                logging.error(f'Error while mkdir for {rel_p}: {e}')
                return
        else:
            logging.info(f"dry_run: make directory (recursive) {dst_path}")

        src_f = self.manifest_src.entries[rel_p]
        self.manifest_xfer.mark_mkdir(rel_p, src_f)

    @abstractmethod
    def _mkdir(self, rel_p):
        '''Create the directory give by rel_p, recursively'''


class SyncLocalToLocal(SyncStrategy):
    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        dst_path = self.manifest_dst._expand_path(rel_p)

        logging.debug(f'rm_file({dst_path})')
        os.unlink(dst_path)

    def _mkdir(self, rel_p):
        '''Create an empty directory'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        os.makedirs(dst_path, exist_ok=True)

    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        logging.debug(f'cp {src_path} {dst_path}')

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        shutil.copy(src_path, dst_temp)

        os.rename(dst_temp, dst_path)

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        logging.debug(f'metadata copy {src_path} {dst_path}')

        src_f = self.manifest_src.entries[rel_p]
        dst_f = self.manifest_xfer.entries[rel_p]

        os.chown(dst_path, src_f.metadata["uid"], src_f.metadata["gid"])
        os.chmod(dst_path, src_f.metadata["mode"])
        os.utime(dst_path, ns=src_f.get_utime())
        dst_f.mark_metadata_transferred(src_f)


class SyncLocalToS3(SyncStrategy):
    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        dst_path = self.manifest_dst._expand_path(rel_p)

        logging.debug(f'rm_file({dst_path})')
        logging.warn(f'Delete from S3 is not yet supported')

    def _mkdir(self, rel_p):
        '''Create an empty directory

        Note that this is non-sensical in S3, so we don't actually do
        anything here.
        '''
        return

    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        src_lf = self.manifest_src.entries[rel_p]

        bucket = self.manifest_dst.bucket_name

        response = self.manifest_dst.upload_file(src_path, dst_path)

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p

        We store all metadata for S3 hosted files in the manifest file
        itself, so this merely transfers from one manifest to the other.
        '''
        src_f = self.manifest_src.entries[rel_p]

        self.manifest_xfer.mark_transferred(rel_p, src_f)


class SyncS3ToLocal(SyncLocalToLocal):
    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        response = self.manifest_src.download_file(src_path, dst_temp)

        os.rename(dst_temp, dst_path)

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        logging.debug(f'metadata copy {src_path} {dst_path}')

        src_f = self.manifest_src.entries[rel_p]
        dst_f = self.manifest_xfer.entries[rel_p]

        os.chown(dst_path, src_f.metadata["uid"], src_f.metadata["gid"])
        os.chmod(dst_path, src_f.metadata["mode"])
        os.utime(dst_path, ns=src_f.get_utime())
        dst_f.mark_metadata_transferred(src_f)
