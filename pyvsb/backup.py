"""Controls backup creation and restoring."""

import bz2
import copy
import errno
import logging
import os
import stat
import tarfile

import psys

from . import utils
from .core import Error
from .storage import Storage

LOG = logging.getLogger(__name__)


_STATE_OPENED = "opened"
"""Opened object state."""

_STATE_COMMITTED = "committed"
"""Committed object state."""

_STATE_CLOSED = "closed"
"""Closed object state."""


_DATA_FILE_NAME = "data.tar"
"""Name of backup data file."""

_METADATA_FILE_NAME = "metadata.bz2"
"""Name of backup metadata file."""


_ENCODING = "utf-8"
"""Encoding for all written files."""


_FILE_STATUS_EXTERN = "extern"
"""Extern file status."""

_FILE_STATUS_UNIQUE = "unique"
"""Unique file status."""



class Backup:
    """Controls backup creation."""

    def __init__(self, config, storage):
        # Backup config
        self.__config = config

        # Backup storage abstraction
        self.__storage = storage

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


        # A set of all files added to the backup
        self.__files = set()

        # Inodes of hard links added to the backup (to track hard-linked files)
        self.__hardlink_inodes = {}


        try:
            self.__group, self.__name, path = self.__storage.create_backup(
                self.__config["max_backups"])

            self.__load_all_backup_metadata(self.__config["trust_modify_time"])


            LOG.debug("Creating backup %s in group %s...", self.__name, self.__group)

            try:
                self.__data = utils.CompressedTarFile(
                    os.path.join(path, _DATA_FILE_NAME),
                    write = self.__config["compression"])
            except Exception as e:
                raise Error("Unable to create a backup data tar archive in '{}': {}.", path, psys.e(e))

            metadata_path = os.path.join(path, _METADATA_FILE_NAME)

            try:
                self.__metadata = bz2.BZ2File(metadata_path, mode = "w")
            except Exception as e:
                raise Error("Unable to create a backup metadata file '{}': {}.",
                    metadata_path, psys.e(e))
        except:
            self.close()
            raise


    def add_file(self, path, stat_info, link_target = None, file_obj = None):
        """Adds a file to the backup."""

        if self.__state != _STATE_OPENED:
            raise Error("The backup file is closed")

        # Limitation of tar format
        if "\0" in path:
            raise Error(r"File names with '\0' aren't supported")

        # Limitation due to using text files for metadata
        if "\r" in path or "\n" in path:
            raise Error(r"File names with '\r' or '\n' aren't supported")

        if path in self.__files:
            raise Error("File is already added to the backup")

        self.__files.add(path)


        extern = False

        hard_link = (
            self.__config["preserve_hard_links"] and
            stat.S_ISREG(stat_info.st_mode) and stat_info.st_nlink > 1
        )

        # Find its hard-linked file in the backup
        if hard_link:
            inode = ( stat_info.st_dev, stat_info.st_ino )
            link_target = self.__hardlink_inodes.get(inode)

        has_data = (
            link_target is None and
            file_obj is not None and
            stat_info.st_size
        )

        # Try to deduplicate backed up files
        if has_data:
            file_obj = utils.HashableFile(file_obj)

            fingerprint = _get_file_fingerprint(stat_info)
            file_hash = self.__deduplicate(path, stat_info, fingerprint, file_obj)
            extern = file_hash is not None

        # Add the file to the archive
        tar_info = _get_tar_info(path, stat_info, link_target, extern)
        self.__data.addfile(tar_info, fileobj = file_obj)

        # Write the file's metadata
        if has_data:
            if not extern:
                file_hash = file_obj.hexdigest()
                self.__hashes.add(file_hash)

            self.__write_file_metadata(path, file_hash, fingerprint, extern)

        if hard_link and link_target is None:
            self.__hardlink_inodes[inode] = path


    def close(self):
        """Closes the object."""

        if self.__state == _STATE_CLOSED:
            return

        try:
            if (
                self.__name is not None and
                self.__group is not None and
                self.__state != _STATE_COMMITTED
            ):
                try:
                    self.__close()
                except Exception as e:
                    LOG.error("Failed to close '%s' backup object: %s",
                        self.__name, psys.e(e))

                self.__storage.cancel_backup(self.__group, self.__name)
        finally:
            self.__state = _STATE_CLOSED


    def commit(self):
        """Commits the changes."""

        if self.__state != _STATE_OPENED:
            raise Error("The backup file is closed.")

        try:
            self.__close()

            self.__storage.commit_backup(self.__group, self.__name)
            self.__state = _STATE_COMMITTED

            self.__storage.rotate_groups(self.__config["max_backup_groups"])
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
                    self.__data = None
        finally:
            if self.__metadata is not None:
                try:
                    self.__metadata.close()
                except Exception as e:
                    raise Error("Unable to close backup metadata file: {}.", psys.e(e))
                finally:
                    self.__metadata = None


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

        while file_size < stat_info.st_size:
            data = file_obj.read(
                min(psys.BUFSIZE, stat_info.st_size - file_size))

            if data:
                file_size += len(data)
            elif file_size == stat_info.st_size:
                break
            else:
                raise Error("The file has been truncated during the backup.")

        file_hash = file_obj.hexdigest()
        file_obj.reset()

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
                self.__prev_files.setdefault(path, ( hash, fingerprint ))

        _load_metadata(self.__storage.backup_path(self.__group, name), handle_metadata)


    def __write_file_metadata(self, path, file_hash, fingerprint, extern):
        """Writes the specified file metadata."""

        metadata = "{hash} {status} {fingerprint} {path}\n".format(
            hash = file_hash, fingerprint = fingerprint, path = path,
            status = _FILE_STATUS_EXTERN if extern else _FILE_STATUS_UNIQUE)

        self.__metadata.write(metadata.encode(_ENCODING))



class Restore:
    """Controls backup restoring."""

    def __init__(self, backup_path, restore_path = None, in_place = False):
        # Backup name
        self.__name = None

        # Backup group name
        self.__group = None

        # Backup data storage abstraction
        self.__storage = None

        # Restore path
        self.__restore_path = restore_path

        # Don't use extra disc space by decompressing backup files
        self.__in_place = in_place

        # Current object state
        self.__state = _STATE_OPENED


        # Data file
        self.__data = None

        # Extern files
        self.__extern_files = {}

        # All backups with extern files with cached metadata
        self.__backups = []

        # False if something went wrong during the restore
        self.__ok = True


        try:
            LOG.info("Restoring backup '%s'...", backup_path)

            self.__name, self.__group, self.__storage = Storage.create(backup_path)
            if self.__restore_path is None:
                self.__restore_path = self.__name

            try:
                self.__data = utils.CompressedTarFile(
                    os.path.join(backup_path, _DATA_FILE_NAME),
                    decompress = not self.__in_place)
            except Exception as e:
                raise Error("Unable to open data of '{}' backup: {}.",
                    backup_path, psys.e(e))

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
            for backup in self.__backups:
                if backup["data"] is not self.__data:
                    try:
                        backup["data"].close()
                    except Exception as e:
                        LOG.error("Failed to close a data file of '%s' backup: %s.", backup["name"], e)

            del self.__backups[:]

            if self.__data is not None:
                try:
                    self.__data.close()
                except Exception as e:
                    LOG.error("Failed to close a data file of '%s' backup: %s.", self.__name, e)
                finally:
                    self.__data = None

            self.__extern_files.clear()
        finally:
            self.__state = _STATE_CLOSED


    def restore(self, paths_to_restore = None):
        """Restores the backup.

        Returns True if all files has been successfully restored.
        """

        if self.__state != _STATE_OPENED:
            raise Error("The backup file is closed.")


        try:
            os.mkdir(self.__restore_path, 0o700)
        except Exception as e:
            raise Error("Unable to create restore directory '{}': {}.",
                self.__restore_path, psys.e(e))


        files = []

        LOG.debug("Loading the backup's data...")

        try:
            for tar_info in self.__data:
                files.append(tar_info)
        except Exception as e:
            LOG.error("Failed to load the backup's data: %s.", psys.e(e))
            self.__ok = False
        else:
            LOG.debug("The backup's data has been successfully loaded")


        directories = []

        for tar_info in files:
            path = "/" + tar_info.name

            if paths_to_restore is not None:
                for path_to_restore in paths_to_restore:
                    if path == path_to_restore or path.startswith(path_to_restore + os.path.sep):
                        break
                else:
                    continue

            restore_path = os.path.join(self.__restore_path, tar_info.name)

            LOG.info("Restoring '%s'...", path)

            try:
                if tar_info.isdir():
                    os.makedirs(restore_path, mode = 0o700)
                    directories.append(tar_info)
                elif tar_info.islnk():
                    target_path = os.path.join(self.__restore_path, tar_info.linkname)

                    try:
                        os.link(target_path, restore_path)
                    except Exception as e:
                        raise Error("Unable to create a hard link to '{}': {}.", target_path, psys.e(e))
                else:
                    extern_hash = self.__extern_files.get(path) if tar_info.isreg() else None

                    try:
                        if extern_hash is None:
                            try:
                                self.__data.extract(tar_info,
                                    path = self.__restore_path, set_attrs = False)
                            except Exception as e:
                                raise Error("Unable to extract the file from backup: {}.", psys.e(e))
                        else:
                            self.__restore_extern_file(tar_info, extern_hash)
                    finally:
                        self.__restore_attributes(tar_info, restore_path)
            except Exception as e:
                LOG.error("Failed to restore '%s': %s", path, psys.e(e))
                self.__ok = False

        directories.sort(key = lambda tar_info: tar_info.name, reverse = True)

        for tar_info in directories:
            self.__restore_attributes(tar_info,
                os.path.join(self.__restore_path, tar_info.name))

        return self.__ok



    def __init_metadata_cache(self):
        """Initializes the backup metadata cache."""

        def handle_metadata(hash, status, fingerprint, path):
            if status == _FILE_STATUS_EXTERN:
                self.__extern_files[path] = hash

        backup_path = self.__storage.backup_path(self.__group, self.__name)
        self.__ok &= _load_metadata(backup_path, handle_metadata)

        if self.__extern_files:
            try:
                backups = self.__storage.backups(self.__group)
            except Exception as e:
                LOG.error("Failed to read metadata for backup group %s: %s", self.__group, e)
            else:
                extern_hashes = set(self.__extern_files.values())

                for name in backups:
                    hashes, paths = self.__load_backup_metadata(
                        self.__storage.backup_path(self.__group, name))

                    hashes &= extern_hashes

                    if hashes:
                        backup = self.__load_backup_data(name, hashes, paths)
                        if backup is not None:
                            self.__backups.append(backup)

                self.__backups.sort(
                    key = lambda backup: len(backup["files"]), reverse = True)

                if self.__backups:
                    LOG.debug("Restoring extern data from the following backups: %s.",
                        ", ".join(backup["name"] for backup in self.__backups))


    def __load_backup_data(self, name, hashes, paths):
        """Loads the specified backup's data."""

        files = {}
        data = None
        backup_path = self.__storage.backup_path(self.__group, name)

        LOG.debug("Loading data of '%s' backup...", backup_path)

        try:
            if name == self.__name:
                data = self.__data
            else:
                data = utils.CompressedTarFile(
                    os.path.join(backup_path, _DATA_FILE_NAME),
                    decompress = not self.__in_place)

            for tar_info in data:
                hash = paths.get("/" + tar_info.name)
                if hash is not None and hash in hashes:
                    files[hash] = tar_info
        except Exception as e:
            LOG.error("Failed to load data of '%s' backup: %s.", backup_path, psys.e(e))
        else:
            LOG.debug("Data of '%s' backup has been successfully loaded.", backup_path)

        if files:
            return {
                "name":  name,
                "files": files,
                "data":  data,
            }
        else:
            if data is not None and data is not self.__data:
                try:
                    data.close()
                except Exception as e:
                    LOG.error("Failed to close data file of '%s' backup: %s.", backup_path, e)

            return None


    def __load_backup_metadata(self, backup_path):
        """Loads metadata for the specified backup."""

        paths = {}
        hashes = set()

        def handle_metadata(hash, status, fingerprint, path):
            if status == _FILE_STATUS_UNIQUE:
                paths[path] = hash
                hashes.add(hash)

        _load_metadata(backup_path, handle_metadata)

        return hashes, paths


    def __restore_attributes(self, tar_info, path):
        """Restores all attributes of a restored file."""

        if os.geteuid() == 0:
            try:
                try:
                    uid = utils.getpwnam(tar_info.uname)[2]
                except KeyError:
                    uid = tar_info.uid

                try:
                    gid = utils.getgrnam(tar_info.gname)[2]
                except KeyError:
                    gid = tar_info.gid

                os.lchown(path, uid, gid)
            except Exception as e:
                if not psys.is_errno(e, errno.ENOENT):
                    LOG.error("Failed to set owner of '%s': %s.", path, psys.e(e))
                    self.__ok = False

        if not tar_info.issym():
            try:
                os.chmod(path, tar_info.mode)
            except Exception as e:
                if not psys.is_errno(e, errno.ENOENT):
                    LOG.error("Failed to change permissions of '%s': %s.", path, psys.e(e))
                    self.__ok = False

            try:
                os.utime(path, ( tar_info.mtime, tar_info.mtime ))
            except Exception as e:
                if not psys.is_errno(e, errno.ENOENT):
                    LOG.error("Failed to change access and modification time of '%s': %s.", path, psys.e(e))
                    self.__ok = False


    def __restore_extern_file(self, tar_info, file_hash):
        """Restores the specified extern file."""

        LOG.debug("Looking up for extern file '%s' with hash %s...",
            tar_info.name, file_hash)

        for backup in self.__backups:
            extern_tar_info = backup["files"].get(file_hash)
            if extern_tar_info is None:
                continue

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



def _get_file_fingerprint(stat_info):
    """Returns fingerprint of a file by its stat() info."""

    return "{device}:{inode}:{mtime}".format(
        device = stat_info.st_dev, inode = stat_info.st_ino,
        mtime = int(stat_info.st_mtime))


def _get_tar_info(path, stat_info, link_target = None, extern = False):
    """Returns a TarInfo object for the specified file."""

    tar_info = tarfile.TarInfo()
    stat_mode = stat_info.st_mode

    if stat.S_ISREG(stat_mode):
        if link_target is None:
            tar_info.type = tarfile.REGTYPE
        else:
            tar_info.type = tarfile.LNKTYPE
            link_target = link_target.lstrip("/")
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
    if link_target is not None:
        tar_info.linkname = link_target

    if tar_info.type == tarfile.REGTYPE and not extern:
        tar_info.size = stat_info.st_size
    else:
        tar_info.size = 0

    try:
        tar_info.uname = utils.getpwuid(stat_info.st_uid)[0]
    except KeyError:
        pass

    try:
        tar_info.gname = utils.getgrgid(stat_info.st_gid)[0]
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

    LOG.debug("Loading backup metadata '%s'...", metadata_path)

    try:
        with bz2.BZ2File(metadata_path, mode = "r") as metadata_file:
            for line in metadata_file:
                line = line.rstrip(b"\r\n")
                if not line:
                    continue

                handle_metadata(*line.decode(_ENCODING).split(" ", 3))

        ok = True
    except Exception as e:
        LOG.error("Failed to load backup metadata '%s': %s.", metadata_path, psys.e(e))
    else:
        LOG.debug("Backup metadata '%s' has been successfully loaded.", metadata_path)

    return ok
