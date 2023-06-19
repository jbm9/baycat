from abc import ABC, abstractmethod
import logging
import os
import os.path
import pathlib
import shutil
import tempfile


class SyncStrategy(ABC):
    def __init__(self, manifest_src, manifest_dst, enable_delete=False, dry_run=False):
        self.manifest_src = manifest_src
        self.manifest_dst = manifest_dst
        self.manifest_xfer = manifest_dst.copy()

        self.enable_delete = enable_delete
        self.dry_run = dry_run

    @abstractmethod
    def sync(self):
        '''Actually runs the sync process.
        '''

    @abstractmethod
    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''

    @abstractmethod
    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''

    @abstractmethod
    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''


class SyncLocalToLocal(SyncStrategy):
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

        if self.enable_delete:
            for rel_p in self.diff_state["deleted"]:
                self._rm_file(rel_p)
                paths_touched.append(rel_p)
        elif self.dry_run:
            for rel_p in self.diff_state["deleted"]:
                logging.info(f"dry_run: delete disabled, so not removing {rel_p}")

        # Create all the directories so our files have homes
        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if not lf.is_dir:
                continue
            self._mkdir(rel_p)

            # Add the dir to the list of "files" touched
            paths_touched.append(rel_p)

            # And mark it explicitly as a directory to fixup
            n_comps = len(rel_p.split("/"))
            dir_fixups.add((-n_comps, rel_p))

        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if lf.is_dir:
                continue

            self._transfer_file(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["contents"]:
            self._transfer_file(rel_p)
            self._transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["metadata"]:
            self._transfer_metadata(rel_p)
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

        # And finally play back all the metadata, in order of
        # decreasing depth.
        for _, d in sorted(list(dir_fixups)):
            self._transfer_metadata(d)

        return self.manifest_xfer

    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        if self.dry_run:
            logging.info(f"dry_run: remove {dst_path}")
            return
        logging.debug(f'rm_file({dst_path})')
        os.unlink(dst_path)
        self.manifest_xfer.mark_deleted(rel_p)

    def _mkdir(self, rel_p):
        '''Create an empty directory'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        if self.dry_run:
            logging.info(f"dry_run: make directory (recursive) {dst_path}")
            return

        os.makedirs(dst_path, exist_ok=True)
        self.manifest_xfer.mark_mkdir(rel_p)

    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        src_lf = self.manifest_src.entries[rel_p]
        if src_lf.is_dir:
            return
        if self.dry_run:
            logging.info(f"dry_run: transfer {src_path} -> {dst_path}")
            return

        logging.debug(f'cp {src_path} {dst_path}')

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        shutil.copy(src_path, dst_temp)

        os.rename(dst_temp, dst_path)
        os.utime(dst_path, ns=src_lf.get_utime())

        if rel_p in self.manifest_dst.entries:
            self.manifest_xfer.entries[rel_p].mark_contents_transferred(src_lf)
        else:
            self.manifest_xfer.entries[rel_p] = src_lf.copy()

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        if self.dry_run:
            logging.info(f"dry_run: transfer metadata {src_path} -> {dst_path}")
            return

        logging.debug(f'metadata copy {src_path} {dst_path}')

        src_lf = self.manifest_src.entries[rel_p]
        dst_lf = self.manifest_xfer.entries[rel_p]

        os.chown(dst_path, src_lf.metadata["uid"], src_lf.metadata["gid"])
        os.chmod(dst_path, src_lf.metadata["mode"])
        os.utime(dst_path, ns=src_lf.get_utime())
        dst_lf.mark_metadata_transferred(src_lf)


class SyncLocalToS3(SyncStrategy):
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

        if self.enable_delete:
            for rel_p in self.diff_state["deleted"]:
                self._rm_file(rel_p)
                paths_touched.append(rel_p)
        elif self.dry_run:
            for rel_p in self.diff_state["deleted"]:
                logging.info(f"dry_run: delete disabled, so not removing {rel_p}")

        # Create all the directories so our files have homes
        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if not lf.is_dir:
                continue
            self._mkdir(rel_p)

            # Add the dir to the list of "files" touched
            paths_touched.append(rel_p)

            # And mark it explicitly as a directory to fixup
            n_comps = len(rel_p.split("/"))
            dir_fixups.add((-n_comps, rel_p))

        for rel_p in self.diff_state["added"]:
            lf = self.manifest_src.entries[rel_p]
            if lf.is_dir:
                continue

            self._transfer_file(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["contents"]:
            self._transfer_file(rel_p)
            self._transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["metadata"]:
            self._transfer_metadata(rel_p)
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

        # And finally play back all the metadata, in order of
        # decreasing depth.
        for _, d in sorted(list(dir_fixups)):
            self._transfer_metadata(d)

        return self.manifest_xfer

    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        if self.dry_run:
            logging.info(f"dry_run: remove {dst_path}")
            return
        logging.debug(f'rm_file({dst_path})')
        os.unlink(dst_path)
        self.manifest_xfer.mark_deleted(rel_p)

    def _mkdir(self, rel_p):
        '''Create an empty directory'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        if self.dry_run:
            logging.info(f"dry_run: make directory (recursive) {dst_path}")
            return

        os.makedirs(dst_path, exist_ok=True)
        self.manifest_xfer.mark_mkdir(rel_p)

    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst._expand_path(rel_p)
        src_path = self.manifest_src._expand_path(rel_p)

        src_lf = self.manifest_src.entries[rel_p]
        if src_lf.is_dir:
            return
        if self.dry_run:
            logging.info(f"dry_run: transfer {src_path} -> {dst_path}")
            return

        logging.debug(f'cp {src_path} {dst_path}')

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        shutil.copy(src_path, dst_temp)

        os.rename(dst_temp, dst_path)
        os.utime(dst_path, ns=src_lf.get_utime())

        if rel_p in self.manifest_dst.entries:
            self.manifest_xfer.entries[rel_p].mark_contents_transferred(src_lf)
        else:
            self.manifest_xfer.entries[rel_p] = src_lf.copy()

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p

        We store all metadata for S3 hosted files in the manifest file
        itself, so this merely transfers from one manifest to the other.
        '''

        if self.dry_run:
            logging.info(f"dry_run: transfer metadata local:{rel_p} -> S3:{rel_p}")
            return

        logging.debug(f'metadata copy {src_path} {dst_path}')

        src_lf = self.manifest_src.entries[rel_p]
        dst_lf = self.manifest_xfer.entries[rel_p]

        dst_lf.mark_metadata_transferred(src_lf)
