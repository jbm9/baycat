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
    '''Base class for all synchronization strategies

    If you want to add a new pair of (source type, destination type)
    manifests for transfers, you'll need to subclass this and
    implement the "filesystem operation" abstract methods.  The Local
    and S3 examples are reasonably concise examples to work from.
    '''
    def __init__(self, manifest_src, manifest_dst,
                 enable_delete=False, dry_run=False, verbose=False,
                 overwrite_regressed=False):
        '''Set up for synchronization

        * manifest_src: the Manifest-y thing to source files from
        * manifest_dst: the Manifest-y thing to send files to
        * enable_delete: allow the sync process to delete things from
                         the destination
        * dry_run: do a "dry run" (print what would happen, but make
                   no changes)
        * verbose: print out synchronization actions as they happen
        * overwrite_regressed: if True, allows overwriting files despite
                            local changes

        The src and dst manifests can be anything that acts
        sufficiently like a Manifest.  We don't have a good API for
        that yet, so you'll need to poke around a bit.  Apologies, but
        I don't think the current architecture is fully baked enough
        yet, so it's not worth documenting the specifics ahead of
        time.

        '''
        self.manifest_src = manifest_src
        self.manifest_dst = manifest_dst

        # Create a version of the destination we can twiddle as we go
        # through the transfer, then save at the end of the process.
        self.manifest_xfer = manifest_dst.copy()

        logging.debug(f'Sync: "{manifest_src.root}" -> "{manifest_dst.root}"')

        self.enable_delete = enable_delete
        self.dry_run = dry_run
        self.verbose = verbose
        self.overwrite_regressed = overwrite_regressed

        if self.dry_run and self.enable_delete:
            logging.warning(f'Delete was enabled in a dry run; disabling it.')
            self.enable_delete = False

        self._counters = defaultdict(int)

        self._dirty = False  # If we got any exceptions during the process

    def was_success(self):
        '''Call this after a run to check if any problems were experienced
        '''
        return not self._dirty

    def sync(self, dry_run=False):
        '''Run the sync process

        This is a complicated process, but has a reasonable number of
        inline comments.  The gist of it is that we walk the current
        state of the source manifest (so you need to update it before
        calling this) and the current state of the destination
        manifest (same caveat), compute the difference in state, and
        then walk through making that happen.

        If deletions are enabled, they happen before anything else.
        This ordering is important, as we may be space-limited, and
        need to free up that space on the remote side before creating
        our new files.  Even if we don't have capacity problems on the
        far side, it's a reasonably cheap operation to lead with.

        The exact order in which new/updated files are transferred is
        touchy on a local posix filesystem.  Specifically, we need to
        ensure that all subdirectories exist before creating any
        files.  To avoid complications here, we just create all the
        subdirectories up front.  In the future, it may make sense to
        do a topo sort and create directories as needed, but it's
        reasonably cheap to do up front, and will probably smoke out
        intermittent problems with the destination reasonably quickly.

        Finally, at the end of the process, we have to go through and
        touch up all the metadata for the directories, as their
        timestamps are disturbed when we create files within them.

        The timestamp of the root directory will be affected by this
        process (unless you move the .baycat_manifest file elsewhere).
        We do not "sweep away" that specific case of mtime
        disturbance, as doing so would be unintentionally covering up
        the fact that baycat has run in the first place.  We want to
        be honest about the fact that we have changed the directory's
        contents.
        '''
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
            if rel_p in self.diff_state["regressed"] and not self.overwrite_regressed:
                logging.warning(f'Skipping locally updated file "{rel_p}"')
                continue
            self.transfer_file(rel_p)
            self.transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        for rel_p in self.diff_state["metadata"]:
            if rel_p and rel_p in self.diff_state["regressed"] and not self.overwrite_regressed:
                logging.warning(f'Skipping locally updated file "{rel_p}"')
                continue

            self.transfer_metadata(rel_p)
            paths_touched.append(rel_p)

        # Recursively add parent directories to the fixup queue
        for rel_p in paths_touched:
            p_cur = rel_p
            while p_cur and p_cur != "/":
                p_cur = os.path.dirname(p_cur)
                n_comps = len(p_cur.split("/"))
                if p_cur and p_cur != "/":
                    dir_fixups.add((-n_comps, p_cur))

        # And finally play back all the metadata, in order of
        # decreasing depth.  Which is why we use the weird tuple.
        for _, d in sorted(list(dir_fixups)):
            if d not in self.diff_state["deleted"]:
                self.transfer_metadata(d)

        return self.manifest_xfer

    def rm_file(self, rel_p):
        '''Delete a file (respects dry_run and enable_delete)

        rel_p the relative path to the file to delete from dst, if allowed

        This method actively checks the enable_delete flag and dry_run
        flag, only calling the actual implementation of the deletion
        method (`_rm_file(rel_p)`) if determines it is allowed to do
        so.

        All errors are consumed within this function, though it only
        calls `mark_deleted()` on the final manifest if the deletion
        is successful.  This ensures that the final manifest does not
        show that the file was deleted, and the deletion can be
        attempted again in the next run.

        In case of an error, this method sets the `_dirty` flag on our
        object, which can be used via `was_success()` after the sync
        is completed.
        '''
        logging.debug('%s.rm_file(%s)' % (type(self).__name__, rel_p))
        if self.enable_delete:
            self._counters["rm"] += 1
            try:
                if self.verbose:
                    logging.info(f'Removing file {rel_p}')

                self._rm_file(rel_p)
            except Execption as e:
                logging.error(f'Error while deleting {rel_p}: {e}')
                self._dirty = True
                return
        else:
            self._counters["delete_skipped"] += 1

            if self.dry_run:
                logging.info(f"dry_run: remove {rel_p}")

        self.manifest_xfer.mark_deleted(rel_p)

    @abstractmethod
    def _rm_file(self, rel_p):
        '''Actually delete a file

        rel_p the relative path to the file to delete from dst, if allowed

        If anything fails, this must raise a reasonable exception
        '''

    def transfer_file(self, rel_p):
        '''Transfer a file from source to dst

        This method actively checks the dry_run flag, only calling the
        actual implementation of the deletion method
        (`_rm_file(rel_p)`) if determines it is allowed to do so.

        All errors are consumed within this function, though it only
        calls `mark_transferred()` on the final manifest if the
        deletion is successful.  This ensures that the final manifest
        does not show that the file was uploaded, and the transfer can
        be attempted again in the next run.

        In case of an error, this method sets the `_dirty` flag on our
        object, which can be used via `was_success()` after the sync
        is completed.
        '''
        logging.debug('%s.transfer_file(%s)' % (str(type(self).__name__), rel_p))
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

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
                self._dirty = True
                return
        elif self.dry_run:
            logging.info(f"dry_run: transfer {src_path} -> {dst_path}")

        self.manifest_xfer.mark_transferred(rel_p, src_lf)

    @abstractmethod
    def _transfer_file(self, rel_p):

        '''Copy file from the src to the dst

        rel_p the relative path to the file to transfer

        If anything fails, this must raise a reasonable exception
        '''

    def transfer_metadata(self, rel_p):
        '''Transfer a path's metadata from source to dst

        This is used to transfer the metadata to the destination
        storage.  On local destinations, that includes setting all the
        usual posix-y stuff (mtime, ctime, uid, gid, perms). On the S3
        destination, it just updates the manifest, freeze drying the
        local state in the manifest, ready to be pulled out at the
        other end.

        This method actively checks the dry_run flag, only calling the
        actual implementation of the deletion method
        (`_rm_file(rel_p)`) if determines it is allowed to do so.

        All errors are consumed within this function, though it only
        calls `mark_metadata_transferred()` on the final manifest if
        the deletion is successful.  This ensures that the final
        manifest does not show that the file was uploaded, and the
        transfer can be attempted again in the next run.

        In case of an error, this method sets the `_dirty` flag on our
        object, which can be used via `was_success()` after the sync
        is completed.
        '''
        logging.debug('%s.transfer_metadata (%s)' % (type(self).__name__, rel_p))
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

        if not self.dry_run:
            self._counters["xfer_metadata"] += 1
            try:
                if self.verbose:
                    logging.info(f'Transfer metadata {rel_p}')

                self._transfer_metadata(rel_p)
            except Exception as e:
                logging.error(f'Error while transferring metadata for {rel_p}: {e}')
                self._dirty = True
                return
        else:
            logging.info(f"dry_run: transfer metadata {src_path} -> {dst_path}")

        src_lf = self.manifest_src.entries[rel_p]
        self.manifest_xfer.mark_transferred(rel_p, src_lf)

    @abstractmethod
    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p

        rel_p the relative path whose metadata should be transferred

        If anything fails, this must raise a reasonable exception
        '''

    def mkdir(self, rel_p):
        '''Create a new directory in the destination

        This is used to create directories in the destination, in
        order to receive files.  On S3 this is actually meaningless,
        as files are stored under "keys" and not in a proper
        filesystem.  On the posix side, it creates a lot of
        directories with default permissions.  This is then corrected
        in the final set of the sync, when we call
        `transfer_metadata()` on all the directories which were
        touched in the transfer.

        This method actively checks the dry_run flag, only calling the
        actual implementation of the deletion method
        (`_rm_file(rel_p)`) if determines it is allowed to do so.

        All errors are consumed within this function, though it only
        calls `mark_metadata_transferred()` on the final manifest if
        the deletion is successful.  This ensures that the final
        manifest does not show that the file was uploaded, and the
        transfer can be attempted again in the next run.

        In case of an error, this method sets the `_dirty` flag on our
        object, which can be used via `was_success()` after the sync
        is completed.
        '''
        logging.debug('%s.mkdir (%s)' % (type(self).__name__, rel_p))
        dst_path = self.manifest_dst.expand_path(rel_p)

        if not self.dry_run:
            self._counters["mkdir"] += 1
            try:
                if self.verbose:
                    logging.info(f'mkdir {rel_p}')

                self._mkdir(rel_p)
            except Exception as e:
                logging.error(f'Error while mkdir for {rel_p}: {e}')
                self._dirty = True
                return
        else:
            logging.info(f"dry_run: make directory (recursive) {dst_path}")

        src_f = self.manifest_src.entries[rel_p]
        self.manifest_xfer.mark_mkdir(rel_p, src_f)

    @abstractmethod
    def _mkdir(self, rel_p):
        '''Create the directory give by rel_p, recursively

        rel_p the relative path to the directory to make

        If anything fails, this must raise a reasonable exception
        '''


class SyncLocalToLocal(SyncStrategy):
    def _rm_file(self, rel_p):
        '''rel_p the relative path to the file to delete from dst, if allowed'''
        dst_path = self.manifest_dst.expand_path(rel_p)

        logging.debug(f'rm_file({dst_path})')

        if self.manifest_dst.entries[rel_p].is_dir:
            os.rmdir(dst_path)
        else:
            os.unlink(dst_path)

    def _mkdir(self, rel_p):
        '''Create an empty directory'''
        dst_path = self.manifest_dst.expand_path(rel_p)
        os.makedirs(dst_path, exist_ok=True)

    def _transfer_file(self, rel_p):
        '''Copy file from the src to the dst'''
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

        logging.debug(f'transfer {src_path} to {dst_path}')

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        shutil.copy(src_path, dst_temp)

        os.rename(dst_temp, dst_path)

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

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
        dst_path = self.manifest_dst.expand_path(rel_p)

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
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

        src_lf = self.manifest_src.entries[rel_p]

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
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

        # Make a temporary path, which we own
        fd, dst_temp = tempfile.mkstemp(prefix=dst_path, dir="/")
        os.close(fd)  # how delightfully retro feeling

        self.manifest_src.download_file(src_path, dst_temp)

        os.rename(dst_temp, dst_path)

    def _transfer_metadata(self, rel_p):
        '''Copy metadata from src to dst for file at relative path rel_p'''
        dst_path = self.manifest_dst.expand_path(rel_p)
        src_path = self.manifest_src.expand_path(rel_p)

        logging.debug(f'metadata copy {src_path} {dst_path}')

        src_f = self.manifest_src.entries[rel_p]
        dst_f = self.manifest_xfer.entries[rel_p]
        dst_f.path = dst_path  # Update absolute path for directories

        uid = src_f.metadata["uid"]
        if uid is None:
            uid = os.getuid()

        gid = src_f.metadata["gid"]
        if gid is None:
            gid = os.getgid()

        mode = src_f.metadata["mode"]

        os.chown(dst_path, int(uid), int(gid))
        if mode is not None:
            os.chmod(dst_path, mode)

        ut = src_f.get_utime()
        os.utime(dst_path, ns=ut)

        dst_f.mark_metadata_transferred(src_f)
