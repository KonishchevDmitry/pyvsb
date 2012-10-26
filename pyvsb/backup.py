import errno
import logging
import os
import stat

import psys
from psys import eintr_retry

from .core import Error
from .storage import Storage

LOG = logging.getLogger(__name__)

# TODO
class FileTypeChangedError(Exception):
    pass

# TODO
class Backuper:
    def __init__(self):
        self.__items = [ "tests/test_root/etc", "tests/test_root/home" ]
        self.__open_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NOATIME
        self.__storage = Storage()

    def backup(self):
        for item in self.__items:
            self.__backup(item, toplevel = True)


    def __backup(self, path, toplevel = False):
        """Backups the specified path."""

        LOG.debug("Backing up '%s'...", path)

        try:
            stat_info = os.lstat(path)

            if stat.S_ISREG(stat_info.st_mode):
                self.__backup_file(path)
            else:
                if stat.S_ISLNK(stat_info.st_mode):
                    try:
                        link_target = os.readlink(path)
                    except EnvironmentError as e:
                        if e.errno == errno.EINVAL:
                            raise FileTypeChangedError()
                        else:
                            raise
                else:
                    link_target = ""

                self.__storage.add(path, stat_info, link_target = link_target)

            if stat.S_ISDIR(stat_info.st_mode):
                for filename in os.listdir(path):
                    self.__backup(os.path.join(path, filename))
        except FileTypeChangedError as e:
            LOG.error("Failed to backup %s: it has suddenly changed its type during the backup.", path)
        except Exception as e:
            if (
                isinstance(e, EnvironmentError) and
                e.errno in ( errno.ENOENT, errno.ENOTDIR ) and not toplevel
            ):
                LOG.warning("Failed to backup %s: it has suddenly vanished.", path)
            else:
                LOG.error("Failed to backup %s: %s.", path, psys.e(e))


    def __backup_file(self, path):
        """Backups the specified file."""

        try:
            try:
                fd = eintr_retry(os.open)(path, self.__open_flags)
            except EnvironmentError as e:
                # If O_NOATIME flag was specified, but the effective user ID
                # of the caller did not match the owner of the file and the
                # caller was not privileged (CAP_FOWNER), the EPERM will be
                # returned.
                if e.errno == errno.EPERM and self.__open_flags & os.O_NOATIME:
                    # Just disable this flag on a first EPERM error
                    LOG.error("Got EPERM error. Disabling O_NOATIME for file opening operations...") # TODO: debug
                    self.__open_flags &= ~os.O_NOATIME
                    fd = eintr_retry(os.open)(path, self.__open_flags)
                else:
                    raise
        except EnvironmentError as e:
            # When O_NOFOLLOW is specified, indicates that this is a
            # symbolic link.
            if e.errno == errno.ELOOP:
                raise FileTypeChangedError()
            else:
                raise

        try:
            file_obj = os.fdopen(fd, "rb")
        except:
            try:
                eintr_retry(os.close)(fd)
            except Exception as e:
                LOG.error("Unable to close a file: %s.", psys.e(e))

            raise

        with file_obj:
            stat_info = os.fstat(file_obj.fileno())
            self.__storage.add(path, stat_info, file_obj = file_obj)
