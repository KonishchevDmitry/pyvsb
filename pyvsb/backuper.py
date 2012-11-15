# TODO
"""Controls backup process."""

import errno
import logging
import os
import stat

import psys
from psys import eintr_retry

import psh
system = psh.Program("sh", "-c", _defer = False)

from .core import LogicalError
from .backup import Backup

LOG = logging.getLogger(__name__)


class FileTypeChangedError(Exception):
    """Raised when a file type has been changed during the backup."""


class Backuper:
    """Controls backup process."""

    def __init__(self, config):
        # Config
        self.__config = config

        # Default open() flags
        self.__open_flags = os.O_RDONLY | os.O_NOFOLLOW
        if hasattr(os, "O_NOATIME"):
            self.__open_flags |= os.O_NOATIME

# TODO
        self.__backup = Backup(config)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


    def backup(self):
        """Starts the backup."""

        try:
            for path, params in self.__config["backup_items"].items():
                self.__run_script(params.get("before"))
                self.__backup_path(path, params.get("filter", []), path)
                self.__run_script(params.get("after"))

            self.__backup.commit()
        finally:
            self.__backup.close()


    def close(self):
        """Closes the object."""

        self.__backup.close()


    def __backup_path(self, path, filters, toplevel):
        """Backups the specified path."""

        LOG.info("Backing up '%s'...", path)

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

                self.__backup.add_file(
                    path, stat_info, link_target = link_target)

            if stat.S_ISDIR(stat_info.st_mode):
                prefix = toplevel + os.path.sep

                for filename in os.listdir(path):
                    file_path = os.path.join(path, filename)

                    for allow, regex in filters:
                        if not file_path.startswith(prefix):
                            raise LogicalError()

                        if regex.search(file_path[len(prefix):]):
                            if allow:
                                self.__backup_path(file_path, filters, toplevel)
                            else:
                                LOG.info("Filtering out '%s'...", file_path)

                            break
                    else:
                        self.__backup_path(file_path, filters, toplevel)
        except FileTypeChangedError as e:
            LOG.error("Failed to backup %s: it has suddenly changed its type during the backup.", path)
        except Exception as e:
            if (
                isinstance(e, EnvironmentError) and
                e.errno in ( errno.ENOENT, errno.ENOTDIR ) and path != toplevel
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
                if (
                    hasattr(os, "O_NOATIME") and e.errno == errno.EPERM and
                    self.__open_flags & os.O_NOATIME
                ):
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
            self.__backup.add_file(path, stat_info, file_obj = file_obj)


    def __run_script(self, script):
        """Runs the specified backup script if it's not None."""

        if script is not None:
            LOG.info("Running: %s", script)

            try:
                system(script)
            except Exception as e:
                LOG.error(e)
