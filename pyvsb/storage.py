import errno
import logging
import os
import time

import psys

from .backup import Backup
from .core import Error

LOG = logging.getLogger(__name__)


# TODO
class Storage:
    """Represents a backup storage."""

    MODE_BACKUP = "backup"
    """Backup mode."""

    MODE_RESTORE = "restore"
    """Restore mode."""


    def __init__(self, config, mode):
        self.__config = config

        try:
            groups = sorted(
                group for group in os.listdir(self.__config["backup_root"])
                    if not group.startswith("."))
        except EnvironmentError as e:
            raise Error("Error while reading backup directory '{}': {}.",
                self.__config["backup_root"], psys.e(e))

        if groups:
            group = groups[-1]
        else:
            group = self.__create_group()

        group_path = self.__get_group_path(group)
        self.__backup = Backup(group_path,
            time.strftime("%Y.%m.%d-%H:%M:%S", time.localtime()),
            Backup.MODE_WRITE, config)


    def add_file(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        # TODO: exceptions
        LOG.debug("Storing file '%s'...", path)
        self.__backup.add_file(path, stat_info, link_target, file_obj)


    # TODO:
    def close(self):
        self.__backup.close()


    def commit(self):
        self.__backup.commit()


    def __create_group(self):
        """Creates a new backup group."""

        group = time.strftime("%Y.%m.%d", time.localtime())
        group_path = self.__get_group_path(group)

        try:
            os.mkdir(group_path)
        except EnvironmentError as e:
            if e.errno != errno.EEXIST:
                raise Error("Unable to create a new backup group directory '{}': {}.",
                    group_path, psys.e(e))

        return group


    def __get_group_path(self, group):
        """Returns path to the specified group."""

        return os.path.join(self.__config["backup_root"], group)
