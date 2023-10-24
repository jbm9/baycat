from collections import defaultdict
import logging
import json
from _md5 import md5
from multiprocessing import Pool
import os
import time

from .json_serdes import JSONSerDes, BaycatJSONEncoder, baycat_json_decoder
from .file_selectors import PathSelector
from .local_file import LocalFile, PATH_DUMMY_FILENAME
from .util import bc_path_join, METADATA_DIRNAME

MANIFEST_FILENAME = "manifest"


class DifferentRootPathException(ValueError):
    '''Raised when an add_selection() can't be added to existing entries'''
    pass


class ManifestAlreadyExists(ValueError):
    '''Raised when a manifest file already exist'''
    pass


class VacuousManifestError(ValueError):
    '''Raised when a manifest with no selectors is operated on'''
    pass


def f_ck(rp_ap):
    '''Internal: Checksum the given (rel_path,abspath) tuple

    returns a (rel_path, hexdigest) tuple
    '''
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

    def __init__(self, path=None, root=None, poolsize=None):
        '''Create a new empty manifest

        * path: path to the .baycat directory to save the manifest in
        * root: path to the directory this manifest is for; None will
          be autopopulated in add_selector()
        * poolsize: the number of subprocesses to use when computing
          checksums.  Note that poolsize=1 is special-cased to not use
          subprocessing
        '''

        self.root = root
        if path is None and root is None:
            raise ValueError('No path or root given, please at least lie to me')

        self.reserved_prefix = None

        if path is None:
            path = Manifest._default_metadata_path(self.root)

        if root and path.startswith(root):
            self.reserved_prefix = path[len(root)+1:] + "/"
        self.path = path

        self.poolsize = poolsize
        self.entries = {}  # path => LocalFile
        self.selectors = []
        self.counters = defaultdict(int)

        logging.debug(f'Manifest created {self} / {self.root} / {self.path}')

    @classmethod
    def _default_metadata_path(cls, root_path):
        '''(internal) Get the default metadata path for a given directory
        '''
        return bc_path_join(root_path, METADATA_DIRNAME)

    @classmethod
    def _default_manifest_path(cls, root_path):
        '''(internal) Get the default manifest path for a given directory
        '''
        return bc_path_join(cls._default_metadata_path(root_path), MANIFEST_FILENAME)

    def is_reserved_path(self, rel_path):
        '''(internal) Check if the given relative path is baycat metadata
        '''
        if not self.reserved_prefix:
            return False

        if rel_path.startswith(self.reserved_prefix):
            return True
        if rel_path == self.reserved_prefix.rstrip('/'):
            return True
        return False

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
        elif self.root != sel.rootpath:
            errstr = f"Manifest rooted at {self.root}, selector at {sel.rootpath}"
            raise DifferentRootPathException(errstr)

        self.selectors.append(sel)
        self.run_selectors(do_checksum)

    def run_selectors(self, do_checksum):
        '''(internal) Run the file selectors and add the files

        * do_checksum: if True, checksums are computed.
        '''
        for sel in self.selectors:
            logging.debug('Manifest run selector %s' % (sel,))
            for lf in sel.walk():
                if self.is_reserved_path(lf.rel_path):
                    logging.debug('Skipping reserved path: %s' % (lf.rel_path,))
                    continue
                logging.debug(f'Manifest {sel.__class__.__name__} add {lf.rel_path}')
                self.entries[lf.rel_path] = lf

        # We do checksums in bulk after walking the filesystem.  This
        # is suboptimal on very large collections, but the code gets
        # particularly hairy if we try and hand out to subprocesses as
        # we go along.  That's worth doing in the future, but for now
        # we're just trying to get it working.

        if do_checksum:
            f_ck_args = [(k, self.expand_path(v.rel_path))
                         for k, v in self.entries.items() if not v.is_dir]

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
    def for_path(cls, root, do_checksum=False, poolsize=None):
        '''Generate an empty manifest, then populate it with a PathSelector

        See the constructor and add_selector() for the parameters for this function.
        '''

        result = Manifest(root=root, poolsize=poolsize)
        ps = PathSelector(root)
        result.add_selector(ps, do_checksum=do_checksum)

        return result

    def expand_path(self, rel_path):
        return os.path.join(self.root, rel_path)

    def save(self, path=None, manifest_path=None, overwrite=False):
        path = path or self.path
        if not self.selectors:
            raise VacuousManifestError('Selectors are required to save a manifest')

        manifest_path = manifest_path or bc_path_join(path, MANIFEST_FILENAME)
        logging.debug(f'{self} Saving manifest to {manifest_path}')

        if not overwrite and os.path.exists(manifest_path):
            raise ManifestAlreadyExists(f"{manifest_path} exists and overwrite not enabled")

        if os.path.exists(path) and not os.path.isdir(path):
            raise ValueError(f'Target path exists, but is not a directory')

        os.makedirs(path, exist_ok=True)

        with open(manifest_path, "w+") as f:
            json.dump(self, f, default=BaycatJSONEncoder().default)

    @classmethod
    def load(cls, root, path=None):
        if path is None:
            path = cls._default_manifest_path(root)

        with open(path, "r") as f:
            d = json.load(f)
            return cls.from_json_obj(d)

    def __eq__(self, m_b):
        cands = [self.__class__, super(self.__class__, self)] + self.__class__.__subclasses__()
        if m_b.__class__ not in cands:
            return NotImplemented

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
            "entries": {},
            "selectors": [],
        }

        for p, lf in self.entries.items():
            result["entries"][p] = lf.to_json_obj()

        for sel in self.selectors:
            result["selectors"].append(sel.to_json_obj())

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

        result.selectors = list(map(baycat_json_decoder, obj["selectors"]))

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

        # Mark if the "old" version has an mtime newer than the "new" one
        files_regressed = []

        for p in files_common:
            lf_new = self.entries[p]
            lf_old = old_manifest.entries[p]

            file_delta = lf_new.delta(lf_old)

            if not file_delta["_dirty"]:
                files_unchanged.append(p)
                continue

            if lf_old.mtime_ns > lf_new.mtime_ns:
                files_regressed.append(p)

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
            "regressed": files_regressed,
            "new_manifest": self,
            "old_manifest": old_manifest,
        }

        return result

    def mark_transferred(self, rel_path, src_f):
        if rel_path in self.entries:
            self.entries[rel_path].mark_contents_transferred(src_f)
            return

        self.entries[rel_path] = src_f.copy()
        self.counters["bytes_up"] += src_f.size

    def mark_deleted(self, rel_path):
        del self.entries[rel_path]

    def mark_mkdir(self, rel_p, src_f):
        self.mark_transferred(rel_p, src_f)

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
        m_minimal = Manifest(root=self.root, poolsize=self.poolsize)
        if not self.selectors:
            raise VacuousManifestError("update called without selectors")

        for sel in self.selectors:
            m_minimal.add_selector(sel)
        m_minimal.run_selectors(do_checksum=False)

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
