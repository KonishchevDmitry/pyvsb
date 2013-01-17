"""Controls backup process."""

import errno
import logging
import os
import stat

import psys
from psys import eintr_retry

import psh
system = psh.Program("sh", "-c", _defer = False)

from .core import Error, LogicalError
from .backup import Backup
from .storage import Storage

LOG = logging.getLogger(__name__)


class FileTypeChangedError(Exception):
    """Raised when a file type has been changed during the backup."""


class Backuper:
    """Controls backup process."""

    def __init__(self, config):
        def get_handler(name):
            handler = config.get(name)

            if handler is None:
                def handler(*args):
                    pass

                return handler

            logger = logging.getLogger("pyvsb.handler." + name)

            def wrapper(*args):
                LOG.info("Executing %s handler...", name)

                try:
                    handler(logger, *args)
                except Exception:
                    LOG.exception("%s handler crashed.", name)
                    self.__ok = False

            return wrapper

        # Config
        self.__config = config

        # False if something went wrong during the backup
        self.__ok = True

        # Default open() flags
        self.__open_flags = os.O_RDONLY | os.O_NOFOLLOW
        if hasattr(os, "O_NOATIME"):
            self.__open_flags |= os.O_NOATIME

        # Backup storage abstraction
        storage = Storage(config["backup_root"],
            on_group_created = get_handler("on_group_created"),
            on_group_deleted = get_handler("on_group_deleted"),
            on_backup_created = get_handler("on_backup_created"))

        # Holds backup writing logic
        self.__backup = Backup(config, storage)

        # A list of backup items' top level directories that has been added to
        # the backup
        self.__toplevel_dirs = set()


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


    def backup(self):
        """Starts the backup."""

        try:
            for path, params in self.__config["backup_items"].items():
                if self.__run_script(params.get("before")):
                    try:
                        self.__add_toplevel_dirs(path)
                    except Exception as e:
                        LOG.error("Failed to backup '%s': %s.", path, psys.e(e))
                        self.__ok = False
                    else:
                        self.__ok &= self.__backup_path(path, params.get("filter", []), path)

                    self.__ok &= self.__run_script(params.get("after"))
                else:
                    self.__ok = False

            self.__backup.commit()
        finally:
            self.__backup.close()

        return self.__ok


    def close(self):
        """Closes the object."""

        self.__backup.close()


    def __add_toplevel_dirs(self, path):
        """
        Adds all top level directories of the specified path to the backup.
        """

        toplevel_dir = "/"

        for directory in path.split(os.path.sep)[1:-1]:
            toplevel_dir = os.path.join(toplevel_dir, directory)
            if toplevel_dir in self.__toplevel_dirs:
                continue

            stat_info = os.lstat(toplevel_dir)

            if not stat.S_ISDIR(stat_info.st_mode):
                raise Error("'{}' is not a directory", toplevel_dir)

            self.__toplevel_dirs.add(toplevel_dir)
            self.__backup.add_file(toplevel_dir, stat_info)


    def __backup_path(self, path, filters, toplevel):
        """Backups the specified path."""

        ok = True
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
                    link_target = None

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
            LOG.error("Failed to backup '%s': it has suddenly changed its type during the backup.", path)
            ok = False
        except Exception as e:
            if psys.is_errno(e, (errno.ENOENT, errno.ENOTDIR)) and path != toplevel:
                LOG.warning("Failed to backup '%s': it has suddenly vanished.", path)
            else:
                LOG.error("Failed to backup '%s': %s.", path, psys.e(e))
                ok = False

        return ok


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
                    LOG.debug("Got EPERM error. Disabling O_NOATIME for file opening operations...")
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

        ok = True

        if script is not None:
            LOG.info("Running: %s", script)

            try:
                system(script)
            except Exception as e:
                LOG.error(e)
                ok = False

        return ok
