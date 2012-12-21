"""Backup data storage abstraction."""

import errno
import logging
import os
import re
import shutil
import time

import psys

from .core import Error

LOG = logging.getLogger(__name__)


_GROUP_NAME_FORMAT = "%Y.%m.%d"
"""Backup group name format."""

_GROUP_NAME_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
"""Backup group name regular expression."""


_BACKUP_NAME_FORMAT = "%Y.%m.%d-%H:%M:%S"
"""Backup name format."""

_BACKUP_NAME_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}-\d{2}:\d{2}:\d{2}$")
"""Backup name regular expression."""



class Storage:
    """Backup data storage abstraction."""

    def __init__(
        self, backup_root, on_group_created = None,
        on_group_deleted = None, on_backup_created = None
    ):
        # Backup root directory
        self.__backup_root = backup_root


        # Event handlers

        if on_group_created is not None:
            self.__on_group_created = on_group_created

        if on_group_deleted is not None:
            self.__on_group_deleted = on_group_deleted

        if on_backup_created is not None:
            self.__on_backup_created = on_backup_created


    def backup_path(self, group, name, temp = False):
        """Returns a path to the specified backup."""

        return os.path.join(self.group_path(group),
            "." + name if temp else name)


    def backups(self, group, check = False, reverse = False, orig_error = False):
        """Returns a list of all backups from the specified group."""

        group_path = self.group_path(group)

        try:
            return sorted(
                ( backup for backup in os.listdir(group_path)
                    if ( _BACKUP_NAME_RE.search(backup) if check else not backup.startswith(".") )),
                reverse = reverse)
        except EnvironmentError as e:
            if orig_error:
                raise e
            else:
                raise Error("Error while reading backup group directory '{}': {}.",
                    group_path, psys.e(e))


    def cancel_backup(self, group, name):
        """Cancels the specified backup."""

        shutil.rmtree(
            self.backup_path(group, name, temp = True),
            onerror = lambda func, path, excinfo:
                LOG.error("Failed to remove backup temporary data '%s': %s.",
                    path, psys.e(excinfo[1])))


    def commit_backup(self, group, name):
        """Commits written backup data."""

        cur_path = self.backup_path(group, name, temp = True)
        new_path = self.backup_path(group, name)

        try:
            os.rename(cur_path, new_path)
        except Exception as e:
            raise Error("Unable to rename backup data directory '{}' to '{}': {}.",
                cur_path, new_path, psys.e(e))

        self.__on_backup_created(group, name, new_path)


    @staticmethod
    def create(backup_path):
        """Creates a Storage object for the specified backup path."""

        backup_name = os.path.basename(backup_path)
        group_path = os.path.dirname(backup_path)
        group_name = os.path.basename(group_path)
        backup_root = os.path.dirname(group_path)

        if group_path == "/":
            raise Error("Invalid backup group directory: {}.", group_path)

        if _BACKUP_NAME_RE.search(backup_name) is None:
            raise Error("'{}' doesn't look like a backup directory.", backup_path)

        if _GROUP_NAME_RE.search(group_name) is None:
            raise Error("'{}' doesn't look like a backup group directory.", group_path)

        return backup_name, group_name, Storage(backup_root)


    def create_backup(self, max_backups):
        """Creates a new backup."""

        name = time.strftime(_BACKUP_NAME_FORMAT, time.localtime())
        LOG.info("Creating a new backup '%s'.", name)

        groups = self.__groups()

        if groups and len(self.backups(groups[-1], check = True)) < max_backups:
            group = groups[-1]
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


    def group_path(self, group):
        """Returns a path to the specified group."""

        return os.path.join(self.__backup_root, group)


    def rotate_groups(self, max_backup_groups):
        """Rotates backup groups."""

        try:
            groups = []

            for group in self.__groups(check = True, reverse = True):
                try:
                    if self.backups(group, check = True, orig_error = True):
                        groups.append(group)
                except EnvironmentError as e:
                    if e.errno == errno.ENOENT:
                        # Just in case: ignore race conditions
                        pass
                    else:
                        LOG.error(
                            "Error while rotating backup groups: "
                            "Unable to read backup group '%s': %s.",
                            group, psys.e(e))

            for group in groups[max_backup_groups:]:
                LOG.info("Removing backup group '%s'...", group)

                shutil.rmtree(self.group_path(group),
                    onerror = lambda func, path, excinfo:
                        LOG.error("Failed to remove '%s': %s.", path, psys.e(excinfo[1])))

                if not os.path.exists(self.group_path(group)):
                    self.__on_group_deleted(group)
        except Exception as e:
            LOG.error("Failed to rotate backup groups: %s", e)


    def __create_group(self):
        """Creates a new backup group."""

        group = time.strftime(_GROUP_NAME_FORMAT, time.localtime())
        LOG.info("Creating backup group '%s'.", group)

        group_path = self.group_path(group)

        try:
            os.mkdir(group_path)
        except EnvironmentError as e:
            if e.errno != errno.EEXIST:
                raise Error("Unable to create a new backup group '{}': {}.",
                    group_path, psys.e(e))

        self.__on_group_created(group)

        return group


    def __groups(self, check = False, reverse = False):
        """Returns a list of all backup groups."""

        try:
            return sorted(
                ( group for group in os.listdir(self.__backup_root)
                    if ( _GROUP_NAME_RE.search(group) if check else not group.startswith(".") )),
                reverse = reverse)
        except EnvironmentError as e:
            raise Error("Error while reading backup root directory '{}': {}.",
                self.__backup_root, psys.e(e))


    def __on_backup_created(self, logger, *args):
        """An empty backup creation handler."""


    def __on_group_created(self, logger, *args):
        """An empty group creation handler."""


    def __on_group_deleted(self, logger, *args):
        """An empty group deletion handler."""
