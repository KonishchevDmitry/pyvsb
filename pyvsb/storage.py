"""Backup storage abstraction."""

import errno
import logging
import os
import shutil
import time

import psys

from .backup import Backup
from .core import Error, LogicalError

LOG = logging.getLogger(__name__)


class Storage:
    """Represents a backup storage."""

    MODE_BACKUP = "backup"
    """Backup mode."""

    MODE_RESTORE = "restore"
    """Restore mode."""


    # TODO
    def __init__(self, config, mode):
        self.__config = config

        if mode == self.MODE_BACKUP:
            groups = self.__get_groups()

            if groups:
                group = groups[-1]
                backups = self.__get_backups(group)

                if len(backups) >= self.__config["max_backups"]:
                    group = self.__create_group()
                else:
                    LOG.info("Using backup group %s.", group)
            else:
                group = self.__create_group()

            self.__backup = Backup(
                self.__get_group_path(group),
                time.strftime("%Y.%m.%d-%H:%M:%S", time.localtime()),
                Backup.MODE_WRITE, config)
        elif mode == self.MODE_RESTORE:
            TODO
        else:
            raise LogicalError()


    def add_file(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        self.__backup.add_file(path, stat_info, link_target, file_obj)


    def close(self):
        """Closes the object."""

        self.__backup.close()


    def commit(self):
        """Commits all written data."""

        self.__backup.commit()

        try:
            groups = []

            for group in self.__get_groups(reverse = True):
                group_path = self.__get_group_path(group)

                try:
                    for backup in os.listdir(group_path):
                        if not backup.startswith("."):
                            groups.append(group)
                            break
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        LOG.error(
                            "Error while rotating backup groups: "
                            "Unable to read backup group directory %s: %s.",
                            group_path, psys.e(e))

            for group in groups[self.__config["max_backup_groups"]:]:
                LOG.info("Removing backup group %s...", group)
                shutil.rmtree(self.__get_group_path(group), onerror = lambda func, path, excinfo:
                    LOG.error("Failed to remove '%s' backup group: %s.", path, psys.e(excinfo[1])))
        except Exception as e:
            LOG.error("Failed to rotate backup groups: %s", e)


    def __create_group(self):
        """Creates a new backup group."""

        group = time.strftime("%Y.%m.%d", time.localtime())
        group_path = self.__get_group_path(group)

        LOG.info("Creating backup group %s.", group)

        try:
            os.mkdir(group_path)
        except EnvironmentError as e:
            if e.errno != errno.EEXIST:
                raise Error("Unable to create a new backup group directory '{}': {}.",
                    group_path, psys.e(e))

        return group


    def __get_backups(self, group):
        """Returns a list of all backups in the specified group."""

        group_path = self.__get_group_path(group)

        try:
            return sorted(backup for backup in os.listdir(group_path)
                    if not backup.startswith("."))
        except EnvironmentError as e:
            raise Error("Error while reading backup group directory '{}': {}.",
                group_path, psys.e(e))


    def __get_groups(self, reverse = False):
        """Returns a list of all backup groups."""

        try:
            return sorted((
                group for group in os.listdir(self.__config["backup_root"])
                    if not group.startswith(".")), reverse = reverse)
        except EnvironmentError as e:
            raise Error("Error while reading backup directory '{}': {}.",
                self.__config["backup_root"], psys.e(e))


    def __get_group_path(self, group):
        """Returns path to the specified group."""

        return os.path.join(self.__config["backup_root"], group)
