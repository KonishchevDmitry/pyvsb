import bz2
import errno
import grp
import logging
import mmap
import os
import pwd
import shutil
import stat
import tarfile

from hashlib import sha1

import psys
from psys import eintr_retry

from .core import Error, LogicalError

LOG = logging.getLogger(__name__)


class Backup:
    """Represents a single backup."""

    MODE_READ = "read"
    """Reading mode."""

    MODE_WRITE = "write"
    """Writing mode."""


    STATE_OPENED = "opened"
    """Opened backup object state."""

    STATE_COMMITTED = "committed"
    """Committed backup state."""

    STATE_CLOSED = "closed"
    """Closed backup object state."""


    def __init__(self, domain_path, name, mode):
        # Backup name
        self.__name = name

        # Backup domain path
        self.__domain_path = domain_path

        # Current backup path
        self.__path = os.path.join(domain_path, "." + name)

        # Maps file hashes to their paths
        self.__files = {}

        # Current object state
        self.__state = self.STATE_OPENED


        LOG.debug("Opening backup %s/%s in %s mode...",
            domain_path, name, mode)

        if mode == self.MODE_READ:
            raise Exception("TODO")
        elif mode == self.MODE_WRITE:
            #try:
            #    domains = sorted(
            #        domain for domain in os.listdir(self.__config["backup_root"])
            #            if not domain.startswith("."))
            #except EnvironmentError as e:
            #    raise Error("Error while reading backup directory '{}': {}.",
            #        self.__config["backup_root"], psys.e(e))

            try:
                os.mkdir(self.__path)
            except Exception as e:
                raise Error("Unable to create a backup directory '{}': {}.",
                    self.__path, psys.e(e))

            try:
                data_path = os.path.join(self.__path, "data")

                try:
                    self.__data = tarfile.open(data_path, "w")
                except Exception as e:
                    raise Error("Unable to create a backup storage tar archive '{}': {}.",
                        data_path, psys.e(e))
            except:
                self.close()
                raise
        else:
            raise LogicalError()


    def add_file(self, path, stat_info, link_target, file_obj):
        """Adds a file to the storage."""

        # Limitation due to using text files for metadata
        if "\n" in path or "\r" in path:
            raise Error(r"File names with '\r' and '\n' aren't supported")

        if file_obj is not None:
            file_hash = sha1()

            while True:
                data = file_obj.read(psys.BUFSIZE)
                if not data:
                    break

                file_hash.update(data)

            file_hash = file_hash.hexdigest()
            file_obj.seek(0)

            copy_path = self.__files.get(file_hash)
            if copy_path is not None:
                LOG.debug("Found a copy of '%s' in this backup: '%s'.", path, copy_path)
                link_target = file_hash
            else:
                self.__files[file_hash] = path

        tar_info = _get_tar_info(path, stat_info, link_target)
        self.__data.addfile(tar_info, fileobj = file_obj)


    # TODO
    def close(self):
        if self.__state != self.STATE_CLOSED:
            try:
                if self.__state != self.STATE_COMMITTED:
                    shutil.rmtree(self.__path)
            finally:
                self.__state = self.STATE_CLOSED


    def commit(self):
        """Commits the changes."""

        if self.__state != self.STATE_OPENED:
            raise Error("Invalid backup state.")

        try:
            self.__state = self.STATE_COMMITTED
        finally:
            self.close()



def _get_tar_info(path, stat_info, link_target):
    """Returns a TarInfo object for the specified file."""

    tar_info = tarfile.TarInfo()
    stat_mode = stat_info.st_mode

    if stat.S_ISREG(stat_mode):
        tar_info.type = tarfile.LNKTYPE if link_target else tarfile.REGTYPE
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

    if tar_info.type == tarfile.REGTYPE:
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
