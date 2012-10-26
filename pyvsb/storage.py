import grp
import logging
import os
import pwd
import stat
import tarfile

LOG = logging.getLogger(__name__)

class Storage:
    def __init__(self):
        self.__storage = tarfile.open("storage", "w")
        self.__inodes = {}

    def add(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        LOG.debug("Storing file '%s'...", path)

        tar_info = _get_tar_info(path, stat_info, link_target)
        self.__storage.addfile(tar_info, fileobj = file_obj)


def _get_tar_info(path, stat_info, link_target):
    """Returns a TarInfo object for the specified file."""

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
