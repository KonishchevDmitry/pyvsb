"""Backup storage abstraction."""

import errno
import logging
import os
import shutil
import time

import psys

from .core import Error

LOG = logging.getLogger(__name__)


# TODO: may be rename to backup group
class Storage:
    """Represents a backup storage."""

    # TODO: all methods
    def __init__(self, backup_root):
        # Backup root directory
        self.__backup_root = backup_root

    # TODO
    def create_backup(self, max_backups):
        """Creates a new backup."""

        name = time.strftime("%Y.%m.%d-%H:%M:%S", time.localtime())
        LOG.info("Creating a new backup '%s'...", name)

        groups = self.__groups()

        if groups:
            group = groups[-1]
            backups = self.backups(group)

            if len(backups) >= max_backups:
                group = self.__create_group()
            else:
                LOG.info("Using backup group %s.", group)
        else:
            group = self.__create_group()

        backup_path = self.backup_path(group, name, temp = True)

        try:
            os.mkdir(backup_path)
        except Exception as e:
            raise Error("Unable to create a backup directory '{}': {}.",
                backup_path, psys.e(e))

        return group, name, backup_path
    def commit_backup(self, group, name):
        cur_path = self.backup_path(group, name, temp = True)
        new_path = self.backup_path(group, name)

        try:
            os.rename(cur_path, new_path)
        except Exception as e:
            raise Error("Unable to rename backup data directory '{}' to '{}': {}.",
                cur_path, new_path, psys.e(e))
#
#        try:
#            groups = []
#
#            for group in self.__groups(reverse = True):
#                group_path = self.group_path(group)
#
#                try:
#                    for backup in os.listdir(group_path):
#                        if not backup.startswith("."):
#                            groups.append(group)
#                            break
#                except EnvironmentError as e:
#                    if e.errno != errno.ENOENT:
#                        LOG.error(
#                            "Error while rotating backup groups: "
#                            "Unable to read backup group directory %s: %s.",
#                            group_path, psys.e(e))
#
#            for group in groups[self.__config["max_backup_groups"]:]:
#                LOG.info("Removing backup group %s...", group)
#                shutil.rmtree(self.group_path(group), onerror = lambda func, path, excinfo:
#                    LOG.error("Failed to remove '%s' backup group: %s.", path, psys.e(excinfo[1])))
#        except Exception as e:
#            LOG.error("Failed to rotate backup groups: %s", e)
    def rollback_backup(self, group, name):
        shutil.rmtree(
            self.backup_path(group, name, temp = True),
            onerror = lambda func, path, excinfo:
                LOG.error("Failed to remove '%s' backup's temporary data '%s': %s.",
                    name, path, psys.e(excinfo[1])))




    # TODO
    @staticmethod
    def from_backup_path(backup_path):
        # TODO: links here and everywhere (rmtree)

        backup_path = os.path.abspath(backup_path)
        backup_name = os.path.basename(backup_path)
        backup_group_path = os.path.dirname(backup_path)
        backup_group_name = os.path.basename(backup_group_path)
        backup_root = os.path.dirname(backup_group_path)

        return backup_name, backup_group_name, Storage(backup_root)


    def add_file(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        self.__backup.add_file(path, stat_info, link_target, file_obj)


    def backup_path(self, group, name, temp = False):
        """Returns path to the specified backup."""

        return os.path.join(self.group_path(group),
            ( "." if temp else "" ) + name)


    def backups(self, group, reverse = False):
        """Returns a list of all backups in the specified group."""

        group_path = self.group_path(group)

        try:
            return sorted(
                backup for backup in os.listdir(group_path)
                    if not backup.startswith("."))
        except EnvironmentError as e:
            raise Error("Error while reading backup group directory '{}': {}.",
                group_path, psys.e(e))


    def close(self):
        """Closes the object."""

        self.__backup.close()


    def group_path(self, group):
        """Returns path to the specified group."""

        return os.path.join(self.__backup_root, group)


    def __create_group(self):
        """Creates a new backup group."""

        group = time.strftime("%Y.%m.%d", time.localtime())
        group_path = self.group_path(group)

        LOG.info("Creating backup group %s.", group)

        try:
            os.mkdir(group_path)
        except EnvironmentError as e:
            if e.errno != errno.EEXIST:
                raise Error("Unable to create a new backup group directory '{}': {}.",
                    group_path, psys.e(e))

        return group


    def __groups(self, reverse = False):
        """Returns a list of all backup groups."""

        try:
            return sorted((
                group for group in os.listdir(self.__backup_root)
                    if not group.startswith(".")), reverse = reverse)
        except EnvironmentError as e:
            raise Error("Error while reading backup directory '{}': {}.",
                self.__backup_root, psys.e(e))
