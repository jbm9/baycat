import logging
import json
from _md5 import md5
from multiprocessing import Pool
import os
import time

from .json_serdes import JSONSerDes, BaycatJSONEncoder, baycat_json_decoder
from .file_selectors import PathSelector
from .local_file import LocalFile, PATH_DUMMY_FILENAME

MANIFEST_FILENAME = ".baycat_manifest"


class DifferentRootPathException(ValueError):
    '''Raised when an add_selection() can't be added to existing entries'''
    pass

class ManifestAlreadyExists(ValueError):
    pass


def f_ck(rp_ap):
    chunksize = 32 * 1024
    rel_path, abspath = rp_ap

    t0 = time.time()
    nbytes = 0

    logging.debug(f' MD5 start PID={os.getpid()} / rel_path={rel_path}')
    with open(abspath, "rb") as f:
        hash = md5()
        while True:
            buf = f.read(chunksize)
            if not buf:
                break
            nbytes += len(buf)
            hash.update(buf)

    t1 = time.time()
    dt = t1 - t0
    if dt == 0:
        dt = 1e-9

    logging.debug(f' MD5 Exit  PID={os.getpid()} / rel_path={rel_path} ({nbytes/1024/1024 / dt:0.1f}MBps)')
    return rel_path, hash.hexdigest()


class Manifest(JSONSerDes):
    JSON_CLASSNAME = "Manifest"

    def __init__(self, path=MANIFEST_FILENAME, root=None, poolsize=None):
        '''Create a new empty manifest

        * path: path to the file to save the manifest in
        * root: path to the directory this manifest is for; None will
          be autopopulated in add_selector()
        * poolsize: the number of subprocesses to use when computing
          checksums.  Note that poolsize=1 is special-cased to not use
          subprocessing

        '''
        self.path = path
        self.root = root
        self.poolsize = poolsize
        self.entries = {}  # path => LocalFile

    def add_selector(self, sel, do_checksum=False):
        '''Add from the file_selector interface selector given

        * sel: a PathSelector (or FileSelector implementation) that
          yields LocalFiles to put in the manifest.  If the manifest
          has a root path set, this selector must be rooted at the
          same directory as the manifest,

        * do_checksum: if True, checksums are computed.  See the
          constructor for details on how these are handed out to
          subprocesses.

        If the current manifest's root directory is unset, this will
        set it to the root of the given selector.

        If do_checksum is unset, we don't force checksums on files
        here.  In general, we defer computing checksums until we need
        to use them (ie: at S3 upload time).  They are very expensive,
        and the vast majority of applications change the mtime or size
        of a file when it's modified on disk.  If your application
        does not, you will need to run checksums to do a complete sync
        of your dataset, and will need to force the checksums.
        '''

        if self.root is None:
            self.root = sel.rootpath
        else:
            if self.root != sel.rootpath:
                errstr = f"Manifest at {self.root}, path selector at {sel.rootpath}"
                raise DifferentRootPathException(errstr)

        for lf in sel.walk():
            logging.debug(f'Manifest add {lf.path}')
            self.entries[lf.rel_path] = lf

        # We do checksums in bulk after walking the filesystem.  This
        # is suboptimal on very large collections, but the code gets
        # particularly hairy if we try and hand out to subprocesses as
        # we go along.  That's worth doing in the future, but for now
        # we're just trying to get it working.

        if do_checksum:
            f_ck_args = [ (k, v.path)
                          for k,v in self.entries.items() if not v.is_dir]

            logging.debug(f'Got {len(f_ck_args)} args')
            if self.poolsize == 1:
                d_cksums = map(f_ck, f_ck_args)
            else:
                with Pool(self.poolsize) as p:
                    # Note that we don't care about order here, so we
                    # can use imap_unordered().  Also, the list() is
                    # required to collect results before the pool goes
                    # out of scope.
                    d_cksums = list(p.imap_unordered(f_ck, f_ck_args))


            for rel_p, cksum in d_cksums:
                self.entries[rel_p].cksum = cksum

    @classmethod
    def for_path(cls, rootpath, path=MANIFEST_FILENAME, do_checksum=False, poolsize=None):
        '''Generate an empty manifest, then populate it with a PathSelector

        See the constructor and add_selector() for the parameters for this function.
        '''
        ps = PathSelector(rootpath)
        result = Manifest(path=path, poolsize=poolsize)
        result.add_selector(ps, do_checksum=do_checksum)

        return result

    def _expand_path(self, rel_path):
        return os.path.join(self.root, rel_path)

    def save(self, path=None, overwrite=False):
        if path is None:
            path = self.path

        if not overwrite and os.path.exists(path):
            raise ManifestAlreadyExists(f"There is already a manifest at {path} and you didn't specify overwriting")

        with open(path, "w+") as f:
            json.dump(self, f, default=BaycatJSONEncoder.default)

    @classmethod
    def load(self, path):
        with open(path, "r") as f:
            result = json.load(f, object_hook=baycat_json_decoder)

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

    def to_json_obj(self):
        result = {
            "_json_classname": self.JSON_CLASSNAME,
            "path": self.path,
            "root": self.root,
            # poolsize is skipped
            "entries": {}
        }

        for p, lf in self.entries.items():
            result["entries"][p] = lf.to_json_obj()

        return result

    @classmethod
    def from_json_obj(cls, obj):
        if "_json_classname" not in obj:
            logging.debug(f'Got a non-JSON-SerDes object in Manifest!')
            raise ValueError(f'Got invalid object!')

        if obj["_json_classname"] != cls.JSON_CLASSNAME:
            logging.debug(f'Got a value of class {obj["_json_classname"]} in Manifeste!')
            raise ValueError(f'Got invalid object!')

        result = Manifest(path=obj["path"], root=obj["root"])
        result.entries = obj["entries"]
        for k, v in result.entries.items():
            if v.__class__ == dict:
                result.entries[k] = baycat_json_decoder(v)
        return result

    def diff_from(self, old_manifest):
        '''Collects a set of file diffs from the given manifest to this one.

        This treats the current manifest as the goal state, with
        old_manifest as the state on-disk.

        Note that this doesn't do any actual work, it just finds what
        work needs to be done at this particular point in time.

        '''

        files_added = []    # Files present now, but missing then
        files_deleted = []  # Files present in the other manifest, but not here

        files_content_changed = []  # Files with content changes
        files_metadata_changed = []  # Files with metadata changes

        files_unchanged = []  # Files that have no changes

        # NB: we're going to use *relative* paths in this codepath,
        # not the absolute paths we use everywhere else.
        paths_self = set(self.entries.keys())
        paths_old = set(old_manifest.entries.keys())

        files_added = list(paths_self - paths_old)
        files_deleted = list(paths_old - paths_self)

        files_common = paths_self & paths_old

        for p in files_common:
            lf_new = self.entries[p]
            lf_old = old_manifest.entries[p]

            file_delta = lf_new.delta(lf_old)

            if not file_delta["_dirty"]:
                files_unchanged.append(p)
                continue

            contents_modified = False
            metadata_modified = False

            for k in ["size", "mtime_ns", "cksum"]:
                if file_delta[k]:
                    if lf_new.is_dir:
                        metadata_modified = True
                    else:
                        contents_modified = True

            for k in lf_new.metadata:
                if k == "atime_ns":
                    continue
                if file_delta[k]:
                    metadata_modified = True

            if contents_modified:
                files_content_changed.append(p)

            if metadata_modified:
                files_metadata_changed.append(p)

        #####################
        # We now have enough information to plan a sync.

        result = {
            "added": files_added,
            "deleted": files_deleted,
            "contents": files_content_changed,
            "metadata": files_metadata_changed,
            "unchanged": files_unchanged,
            "new_manifest": self,
            "old_manifest": old_manifest,
        }

        return result

    def mark_deleted(self, rel_path):
        del self.entries[rel_path]

    def mark_mkdir(self, rel_p):
        dpath = rel_p
        abspath = self._expand_path(rel_p)
        # Just make a new entry, this is cheap on local FS
        self.entries[dpath] = LocalFile.for_abspath(self.root, abspath)

    def update(self, force_checksum=False):
        '''Update this manifest with current FS state as efficiently as possible

        * force_checksum: whether or not to force changed_from() to
          run checksums

        This method exists to update a local manifest with the current
        filesystem state.  Unless checksums are forced, this only
        accounts for metadata changes.  Checksums are cleared for
        anything which has metadata changes, and will be recomputed as
        needed for transfers.
        '''
        m_minimal = Manifest.for_path(self.root, do_checksum=False)

        new_keys = set(m_minimal.entries.keys())
        old_keys = set(self.entries.keys())

        # Handle deletes
        for rel_p in (old_keys - new_keys):
            del self.entries[rel_p]

        # And adds
        for rel_p in (new_keys - old_keys):
            self.entries[rel_p] = m_minimal.entries[rel_p]

        for rel_p in (new_keys & old_keys):
            lf_new = m_minimal.entries[rel_p]
            lf_old = self.entries[rel_p]

            if lf_new.changed_from(lf_old, force_checksum=force_checksum):
                self.entries[rel_p] = m_minimal.entries[rel_p]
