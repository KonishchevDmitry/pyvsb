# TODO
"""Provides a class that represents a backup."""

import bz2
import copy
import grp
import logging
import os
import pwd
import stat
import tarfile

from hashlib import sha1

import psys

from .core import Error, LogicalError
from .storage import Storage

LOG = logging.getLogger(__name__)


_STATE_OPENED = "opened"
"""Opened object state."""

_STATE_COMMITTED = "committed"
"""Committed object state."""

_STATE_CLOSED = "closed"
"""Closed object state."""


_DATA_FILE_NAME = "data.tar.bz2"
"""Name of backup data file."""

_METADATA_FILE_NAME = "metadata.bz2"
"""Name of backup metadata file."""


_ENCODING = "utf-8"
"""Encoding for all written files."""


_FILE_STATUS_EXTERN = "extern"
"""Extern file status."""

_FILE_STATUS_UNIQUE = "unique"
"""Unique file status."""



class Restore:
    """Controls backup restoring."""

    def __init__(self, backup_path, restore_path):
        name, group, storage = Storage.create(backup_path)

        # Backup name
        self.__name = name

        # Backup group name
        self.__group = group

        # Backup storage abstraction
        self.__storage = storage

        # Restore path
        self.__restore_path = restore_path

        # Current object state
        self.__state = _STATE_OPENED


        # Backup data file
        self.__data = None

        # Backup extern files
        self.__extern_files = {}

        # All backups from the backup group with cached metadata
        self.__backups = []

        # False if something went wrong during the restore
        self.__ok = True


        try:
            LOG.debug("Opening backup %s for restoring...", backup_path)

            data_path = os.path.join(backup_path, _DATA_FILE_NAME)

            try:
                self.__data = tarfile.open(data_path, "r:bz2")
            except Exception as e:
                raise Error("Unable to open backup data '{}': {}.",
                    data_path, psys.e(e))

            self.__init_metadata_cache()
        except:
            self.close()
            raise


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


    def close(self):
        """Closes the object."""

        if self.__state == _STATE_CLOSED:
            return

        try:
            if self.__data is not None:
                try:
                    self.__data.close()
                except Exception as e:
                    LOG.error("Failed to close %s backup data file: %s.", self.__name, e)
                finally:
                    self.__data

            self.__extern_files.clear()

            for backup in self.__backups:
                data = backup.get("data")
                if data is not None:
                    try:
                        data.close()
                    except Exception as e:
                        LOG.error("Failed to close %s backup data file: %s.", backup["name"], e)

            del self.__backups[:]
        finally:
            self.__state = _STATE_CLOSED


    def restore(self):
        """Restores the backup.

        Returns True if all files has been successfully restored.
        """

        if self.__state != _STATE_OPENED:
            raise LogicalError()

        files = []

        LOG.debug("Loading the backup's data...")
        try:
            for tar_info in self.__data:
                files.append(tar_info)
        except Exception as e:
            LOG.error("Failed to load the backup's data: %s.", e)
            self.__ok = False
        else:
            LOG.debug("The backup's data has been successfully loaded")

        for tar_info in files:
            path = "/" + tar_info.name
            LOG.info("Restoring '%s'...", path)

            try:
                file_hash = self.__extern_files.get(path)

                if file_hash is None:
                    try:
                        self.__data.extract(tar_info,
                            path = self.__restore_path, set_attrs = False)
                    except Exception as e:
                        raise Error("Unable to extract the file from backup: {}.", e)
                else:
                    self.__restore_extern_file(tar_info, file_hash)

                # TODO
                #file_path = os.path.join(self.__restore_path, tar_info.name)
                # if os.path.exists(file_path)
                #self.chown(tarinfo, targetpath)
                #if not tarinfo.issym():
                #    self.chmod(tarinfo, targetpath)
                #    self.utime(tarinfo, targetpath)
            except Exception as e:
                LOG.error("Failed to restore '%s': %s", path, psys.e(e))
                self.__ok = False

        return self.__ok


    def __get_extern_file(self, backup, file_hash):
        """
        Returns a file by its hash or None if it doesn't exist in this
        backup.
        """

        tar_info = backup["files"].get(file_hash)
        if tar_info is None:
            return None

        if "data" in backup:
            return tar_info
        else:
            # Load the backup's data, because now tar_info points to a file
            # path instead of a tar file object

            self.__load_backup_data(backup)
            return backup["files"].get(file_hash)


    def __init_metadata_cache(self):
        """Initializes the backup metadata cache."""

        def handle_metadata(hash, status, fingerprint, path):
            if status == _FILE_STATUS_EXTERN:
                self.__extern_files[path] = hash

        self.__ok &= _load_metadata(
            self.__storage.backup_path(self.__group, self.__name), handle_metadata)

        try:
            # Sort backups in descending order as the most suitable for
            # looking up extern files
            backups = self.__storage.backups(self.__group, reverse = True)

            # We should look first in the first backup, because it contains
            # all files that haven't changed since the first backup, which
            # probably the most of all backed up files
            backups = backups[-1:] + backups[:-1]

            # TODO: check order
            LOG.debug("Restoring data from the following backups: %s.", ", ".join(backups))
            self.__backups = [{ "name": backup } for backup in backups ]
        except Exception as e:
            LOG.error("Failed to read metadata for backup group %s: %s.", self.__group, e)


    def __load_backup_data(self, backup):
        """Loads the specified backup's data."""

        files = backup["files"]
        files.clear()

        data = None
        data_path = os.path.join(
            self.__storage.backup_path(self.__group, backup["name"]), _DATA_FILE_NAME)

        LOG.debug("Loading backup data '%s'...", data_path)

        try:
            if backup["name"] == self.__name:
                data = self.__data
            else:
                data = tarfile.open(data_path, "r:bz2")

            paths = backup["paths"]

            for tar_info in data:
                hash = paths.get("/" + tar_info.name)
                if hash is not None:
                    files[hash] = tar_info
        except Exception as e:
            LOG.error("Failed to load backup data '%s': %s.", data_path, psys.e(e))
        else:
            LOG.debug("Backup data '%s' has been successfully loaded.", data_path)
        finally:
            backup["data"] = data
            del backup["paths"]


    def __load_backup_metadata(self, backup):
        """Loads metadata for the specified backup."""

        paths = backup["paths"] = {}
        files = backup["files"] = {}

        def handle_metadata(hash, status, fingerprint, path):
            if status == _FILE_STATUS_UNIQUE:
                files[hash] = path
                paths[path] = hash

        backup_path = self.__storage.backup_path(self.__group, backup["name"])
        _load_metadata(backup_path, handle_metadata)


    def __restore_extern_file(self, tar_info, file_hash):
        """Restores the specified extern file."""

        LOG.debug("Looking up for extern file '%s' with hash %s...",
            tar_info.name, file_hash)

        for backup in self.__backups:
            # TODO: load all that match extern files
            if "files" not in backup:
                self.__load_backup_metadata(backup)

            extern_tar_info = self.__get_extern_file(backup, file_hash)
            if extern_tar_info is not None:
                extern_tar_info = copy.copy(extern_tar_info)
                extern_tar_info.name = tar_info.name

                try:
                    backup["data"].extract(extern_tar_info,
                        path = self.__restore_path, set_attrs = False)
                except Exception as e:
                    raise Error("Unable to extract the file from backup: {}.", e)
                else:
                    break
        else:
            raise Error("Unable to find the file: backup is corrupted.")


# TODO
#        directories = [] 
#
#        if members is None:
#            members = self 
#
#        for tarinfo in members:
#            if tarinfo.isdir():
#                # Extract directories with a safe mode.
#                directories.append(tarinfo)
#                tarinfo = copy.copy(tarinfo)
#                tarinfo.mode = 0o700
#            # Do not set_attrs directories, as we will do that further down
#            self.extract(tarinfo, path, set_attrs=not tarinfo.isdir())
#
#        # Reverse sort directories.
#        directories.sort(key=lambda a: a.name)
#        directories.reverse()
#
#        # Set correct owner, mtime and filemode on directories.
#        for tarinfo in directories:
#            dirpath = os.path.join(path, tarinfo.name)
#            try:
#                self.chown(tarinfo, dirpath)
#                self.utime(tarinfo, dirpath)
#                self.chmod(tarinfo, dirpath)
#            except ExtractError as e:
#                if self.errorlevel > 1: 
#                    raise
#                else:
#                    self._dbg(1, "tarfile: %s" % e)

# File
#        if set_attrs:
#            self.chown(tarinfo, targetpath)
#            if not tarinfo.issym():
#                self.chmod(tarinfo, targetpath)
#                self.utime(tarinfo, targetpath)


class Backup:
    """Controls backup creation."""

    def __init__(self, config):
        # Backup config
        self.__config = config

        # Backup storage abstraction
        self.__storage = None

        # Backup name
        self.__name = None

        # Backup group
        self.__group = None

        # Current object state
        self.__state = _STATE_OPENED


        # Backup data file
        self.__data = None

        # Backup metadata file
        self.__metadata = None

        # A set of hashes of all available files in this backup group
        self.__hashes = set()

        # A map of files from the previous backup to their hashes and
        # fingerprints.
        self.__prev_files = {}


        self.__storage = Storage(self.__config["backup_root"])

        self.__group, self.__name, path = self.__storage.create_backup(
            self.__config["max_backups"])

        try:
            self.__load_all_backup_metadata(self.__config["trust_modify_time"])

            LOG.debug("Creating backup %s in group %s...", self.__name, self.__group)

            data_path = os.path.join(path, _DATA_FILE_NAME)

            try:
                self.__data = tarfile.open(data_path, "w:bz2")
            except Exception as e:
                raise Error("Unable to create a backup data tar archive '{}': {}.",
                    data_path, psys.e(e))

            metadata_path = os.path.join(path, _METADATA_FILE_NAME)

            try:
                self.__metadata = bz2.BZ2File(metadata_path, mode = "w")
            except Exception as e:
                raise Error("Unable to create a backup metadata file '{}': {}.",
                    metadata_path, psys.e(e))
        except:
            self.close()
            raise


    def add_file(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        if self.__state != _STATE_OPENED:
            raise Error("The backup file is closed.")

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
                status = _FILE_STATUS_EXTERN if extern else _FILE_STATUS_UNIQUE)

            self.__metadata.write(metadata.encode(_ENCODING))


    # TODO
    def close(self):
        """Closes the object."""

        if self.__state == _STATE_CLOSED:
            return

        try:
            if self.__state != _STATE_COMMITTED:
                try:
                    self.__close()
                except Exception as e:
                    LOG.error("Failed to close '%s' backup object: %s",
                        self.__name, psys.e(e))

                self.__storage.cancel_backup(self.__group, self.__name)
        finally:
            self.__state = _STATE_CLOSED


    # TODO
    def commit(self):
        """Commits the changes."""

        if self.__state != _STATE_OPENED:
            raise Error("The backup file is closed.")

        try:
            self.__close()
            self.__storage.commit_backup(self.__group, self.__name)
            self.__storage.rotate_groups(self.__config["max_backup_groups"])
            self.__state = _STATE_COMMITTED
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
            backups = self.__storage.backups(self.__group, reverse = True)

            for backup_id, backup in enumerate(backups):
                self.__load_backup_metadata(backup,
                    with_prev_files_info = trust_modify_time and not backup_id)
        except Exception as e:
            LOG.error("Failed to load metadata from previous backups: %s.", psys.e(e))


    def __load_backup_metadata(self, name, with_prev_files_info):
        """Loads the specified backup's metadata."""

        def handle_metadata(hash, status, fingerprint, path):
            if status == _FILE_STATUS_UNIQUE:
                self.__hashes.add(hash)

            if with_prev_files_info:
                self.__prev_files[path] = ( hash, fingerprint )

        _load_metadata(self.__storage.backup_path(self.__group, name), handle_metadata)



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


def _load_metadata(backup_path, handle_metadata):
    """Loads metadata of the specified backup."""

    ok = False

    metadata_path = os.path.join(backup_path, _METADATA_FILE_NAME)

    LOG.debug("Load backup metadata '%s'...", metadata_path)

    try:
        with bz2.BZ2File(metadata_path, mode = "r") as metadata_file:
            for line in metadata_file:
                line = line.rstrip(b"\r\n")
                if not line:
                    continue

                handle_metadata(*line.decode(_ENCODING).split(" ", 4))

        ok = True
    except Exception as e:
        LOG.error("Failed to load backup metadata '%s': %s.", metadata_path, psys.e(e))
    else:
        LOG.debug("Backup metadata '%s' has been successfully loaded.", metadata_path)

    return ok


# TODO: cache grp and pwd

# TODO
#def chown(self, tarinfo, targetpath):
#    """Set owner of targetpath according to tarinfo.
#    """
#    if pwd and hasattr(os, "geteuid") and os.geteuid() == 0:
#        # We have to be root to do so.
#        try:
#            g = grp.getgrnam(tarinfo.gname)[2]
#        except KeyError:
#            g = tarinfo.gid
#        try:
#            u = pwd.getpwnam(tarinfo.uname)[2]
#        except KeyError:
#            u = tarinfo.uid
#        try:
#            if tarinfo.issym() and hasattr(os, "lchown"):
#                os.lchown(targetpath, u, g)
#            else:
#                if sys.platform != "os2emx":
#                    os.chown(targetpath, u, g)
#        except EnvironmentError as e:
#            raise ExtractError("could not change owner")
#
#def chmod(self, tarinfo, targetpath):
#    """Set file permissions of targetpath according to tarinfo.
#    """
#    if hasattr(os, 'chmod'):
#        try:
#            os.chmod(targetpath, tarinfo.mode)
#        except EnvironmentError as e:
#            raise ExtractError("could not change mode")
#
#def utime(self, tarinfo, targetpath):
#    """Set modification time of targetpath according to tarinfo.
#    """
#    if not hasattr(os, 'utime'):
#        return
#    try:
#        os.utime(targetpath, (tarinfo.mtime, tarinfo.mtime))
#    except EnvironmentError as e:
#        raise ExtractError("could not change modification time")
