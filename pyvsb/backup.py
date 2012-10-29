# TODO

import bz2
import grp
import logging
import os
import pwd
import shutil
import stat
import tarfile

from hashlib import sha1

import psys

from .core import Error, LogicalError

LOG = logging.getLogger(__name__)


_ENCODING = "utf-8"
"""Encoding for all written files."""


class Backup:
    """Represents a single backup."""

    MODE_READ = "read"
    """Reading mode."""

    MODE_WRITE = "write"
    """Writing mode."""


    __STATE_OPENED = "opened"
    """Opened backup object state."""

    __STATE_COMMITTED = "committed"
    """Committed backup state."""

    __STATE_CLOSED = "closed"
    """Closed backup object state."""


    __DATA_FILE_NAME = "data.tar.bz2"
    """Name of backup data file."""

    __METADATA_FILE_NAME = "metadata.bz2"
    """Name of backup metadata file."""


    __FILE_STATUS_EXTERN = "extern"
    """Extern file status."""

    __FILE_STATUS_UNIQUE = "unique"
    """Unique file status."""


    def __init__(self, domain_path, name, mode, config):
        # Backup name
        self.__name = name

        # Backup domain path
        self.__domain_path = domain_path

        # Current backup path
        self.__path = os.path.join(domain_path, "." + name)

        # Current object state
        self.__state = self.__STATE_OPENED

        # Backup data open mode
        self.__mode = mode


        # Backup data file
        self.__data = None

        # Backup metadata file
        self.__metadata = None

        # A set of hashes of all available files in this backup domain.
        self.__hashes = set()

        # A map of files from the previous backup to their hashes and
        # fingerprints.
        self.__prev_files = {}

        # TODO
        self.__config = config


        LOG.debug("Opening backup %s/%s in %s mode...",
            domain_path, name, mode)

        if mode == self.MODE_READ:
            raise Exception("TODO")
        elif mode == self.MODE_WRITE:
            self.__load_all_backup_metadata(self.__config["trust_modify_time"])

            try:
                os.mkdir(self.__path)
            except Exception as e:
                raise Error("Unable to create a backup directory '{}': {}.",
                    self.__path, psys.e(e))

            try:
                data_path = os.path.join(self.__path, self.__DATA_FILE_NAME)

                try:
                    self.__data = tarfile.open(data_path, "w")
                except Exception as e:
                    raise Error("Unable to create a backup data tar archive '{}': {}.",
                        data_path, psys.e(e))

                metadata_path = os.path.join(self.__path, self.__METADATA_FILE_NAME)

                try:
                    self.__metadata = bz2.BZ2File(metadata_path, mode = "w")
                except Exception as e:
                    raise Error("Unable to create a backup metadata file '{}': {}.",
                        metadata_path, psys.e(e))
            except:
                self.close()
                raise
        else:
            raise LogicalError()


    def add_file(self, path, stat_info, link_target, file_obj):
        """Adds a file to the storage."""

        if self.__state != self.__STATE_OPENED:
            raise Error("The backup file is closed.")

        if self.__mode != self.MODE_WRITE:
            raise Error("The backup is opened in read-only mode.")

        # Limitation due to using text files for metadata
        if "\r" in path or "\n" in path:
            raise Error(r"File names with '\r' and '\n' aren't supported")


        extern = False
        fingerprint = _get_file_fingerprint(stat_info)

        if file_obj is not None:
            file_hash = self.__deduplicate(path, stat_info, fingerprint, file_obj)
            extern = file_hash is not None
            file_obj = _HashableFile(file_obj)

        tar_info = _get_tar_info(path, stat_info, link_target, extern)
        self.__data.addfile(tar_info, fileobj = file_obj)

        if file_obj is not None:
            if not extern:
                file_hash = file_obj.hash()
                self.__hashes.add(file_hash)

            metadata = "{hash} {status} {fingerprint} {path}\n".format(
                hash = file_hash, fingerprint = fingerprint, path = path,
                status = self.__FILE_STATUS_EXTERN if extern else self.__FILE_STATUS_UNIQUE)

            self.__metadata.write(metadata.encode(_ENCODING))


    def close(self):
        """Closes the object."""

        if self.__state != self.__STATE_CLOSED:
            try:
                if self.__state != self.__STATE_COMMITTED:
                    try:
                        self.__close()
                    except Exception as e:
                        LOG.error("Failed to close '%s' backup object: %s",
                            self.__name, psys.e(e))

                    shutil.rmtree(self.__path, onerror = lambda func, path, excinfo:
                        LOG.error("Failed to remove '%s' backup's temporary data '%s': %s.",
                            self.__name, path, psys.e(excinfo[1])))
            finally:
                self.__state = self.__STATE_CLOSED


    def commit(self):
        """Commits the changes."""

        if self.__state != self.__STATE_OPENED:
            raise Error("The backup file is closed.")

        if self.__mode != self.MODE_WRITE:
            raise Error("The backup is opened in read-only mode.")

        try:
            self.__close()

            backup_path = os.path.join(self.__domain_path, self.__name)

            try:
                os.rename(self.__path, backup_path)
            except Exception as e:
                raise Error("Unable to rename backup data directory '{}' to '{}': {}.",
                    self.__path, backup_path, psys.e(e))
            else:
                self.__path = backup_path

            self.__state = self.__STATE_COMMITTED
        finally:
            self.close()


    def __close(self):
        """Closes all opened files."""

        try:
            if self.__data is not None:
                try:
                    self.__data.close()
                except Exception as e:
                    raise Error("Unable to close backup data file: {}.", psys.e(e))
        finally:
            if self.__metadata is not None:
                try:
                    self.__metadata.close()
                except Exception as e:
                    raise Error("Unable to close backup metadata file: {}.", psys.e(e))


    def __deduplicate(self, path, stat_info, fingerprint, file_obj):
        """Tries to deduplicate the specified file.

        Returns its hash if deduplication succeeded.
        """

        # No need to deduplicate empty files
        if stat_info.st_size == 0:
            return

        # Check modify time
        if self.__config["trust_modify_time"]:
            prev_info = self.__prev_files.get(path)

            if prev_info is not None:
                prev_hash, prev_fingerprint = prev_info

                if fingerprint == prev_fingerprint:
                    LOG.debug(
                        "File '%s' hasn't been changed. Make it an extern file with %s hash.",
                        path, prev_hash)

                    return prev_hash

        # Find files with the same hash -->
        file_size = 0
        file_hash = sha1()

        while file_size < stat_info.st_size:
            data = file_obj.read(
                min(psys.BUFSIZE, stat_info.st_size - file_size))

            if data:
                file_size += len(data)
                file_hash.update(data)
            elif file_size == stat_info.st_size:
                break
            else:
                raise Error("The file has been truncated during the backup.")

        file_obj.seek(0)
        file_hash = file_hash.hexdigest()

        if file_hash in self.__hashes:
            LOG.debug("Make '%s' an extern file with %s hash.", path, file_hash)
            return file_hash
        # Find files with the same hash <--


    def __load_all_backup_metadata(self, trust_modify_time):
        """Loads all metadata from previous backups."""

        try:
            backups = sorted((
                backup for backup in os.listdir(self.__domain_path)
                    if not backup.startswith(".")), reverse = True)

            for backup_id, backup in enumerate(backups):
                self.__load_backup_metadata(backup,
                    with_prev_files_info = trust_modify_time and not backup_id)
        except Exception as e:
            LOG.error("Failed to load metadata from previous backups: %s.", psys.e(e))


    def __load_backup_metadata(self, name, with_prev_files_info):
        """Loads the specified backup's metadata."""

        metadata_path = os.path.join(
            self.__domain_path, name, self.__METADATA_FILE_NAME)

        try:
            with bz2.BZ2File(metadata_path, mode = "r") as metadata_file:
                for line in metadata_file:
                    line = line.rstrip(b"\r\n")
                    if not line:
                        continue

                    hash, status, fingerprint, path = \
                        line.decode(_ENCODING).split(" ", 4)

                    if status == self.__FILE_STATUS_UNIQUE:
                        self.__hashes.add(hash)

                    if with_prev_files_info:
                        self.__prev_files[path] = ( hash, fingerprint )
        except Exception as e:
            LOG.error("Failed to load metadata '%s': %s.", metadata_path, psys.e(e))



class _HashableFile():
    """A wrapper for file object that hashes all read data."""

    def __init__(self, file):
        self.__file = file
        self.__hash = sha1()


    def hash(self):
        """Returns read data hash."""

        return self.__hash.hexdigest()


    def read(self, *args, **kwargs):
        """Reads data from the file and hashes the returning value."""

        data = self.__file.read(*args, **kwargs)
        self.__hash.update(data)
        return data



def _get_file_fingerprint(stat_info):
    """Returns fingerprint of a file by its stat() info."""

    return "{device}:{inode}:{mtime}".format(
        device = stat_info.st_dev, inode = stat_info.st_ino,
        mtime = int(stat_info.st_mtime))


def _get_tar_info(path, stat_info, link_target, extern):
    """Returns a TarInfo object for the specified file."""

    # TODO: hard links

    tar_info = tarfile.TarInfo()
    stat_mode = stat_info.st_mode

    if stat.S_ISREG(stat_mode):
        tar_info.type = tarfile.REGTYPE
    elif stat.S_ISDIR(stat_mode):
        tar_info.type = tarfile.DIRTYPE
    elif stat.S_ISLNK(stat_mode):
        tar_info.type = tarfile.SYMTYPE
    elif stat.S_ISFIFO(stat_mode):
        tar_info.type = tarfile.FIFOTYPE
    elif stat.S_ISCHR(stat_mode):
        tar_info.type = tarfile.CHRTYPE
    elif stat.S_ISBLK(stat_mode):
        tar_info.type = tarfile.BLKTYPE
    else:
        raise Exception("File type is not supported")

    tar_info.name = path.lstrip("/")
    tar_info.mode = stat_mode
    tar_info.uid = stat_info.st_uid
    tar_info.gid = stat_info.st_gid
    tar_info.mtime = stat_info.st_mtime
    tar_info.linkname = link_target

    if tar_info.type == tarfile.REGTYPE and not extern:
        tar_info.size = stat_info.st_size
    else:
        tar_info.size = 0

    try:
        tar_info.uname = pwd.getpwuid(stat_info.st_uid)[0]
    except KeyError:
        pass

    try:
        tar_info.gname = grp.getgrgid(stat_info.st_gid)[0]
    except KeyError:
        pass

    if tar_info.type in ( tarfile.CHRTYPE, tarfile.BLKTYPE ):
        tar_info.devmajor = os.major(stat_info.st_rdev)
        tar_info.devminor = os.minor(stat_info.st_rdev)

    return tar_info
