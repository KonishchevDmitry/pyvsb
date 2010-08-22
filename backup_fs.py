#!/usr/bin/python
# -*- coding: utf-8 -*-

#***************************************************************************
#*   PyVSB                                                                 *
#*                                                                         *
#*   Copyright (C) 2008, Konishchev Dmitry                                 *
#*   http://konishchevdmitry.blogspot.com/                                 *
#*                                                                         *
#*   This program is free software; you can redistribute it and/or modify  *
#*   it under the terms of the GNU General Public License as published by  *
#*   the Free Software Foundation; either version 3 of the License, or     *
#*   (at your option) any later version.                                   *
#*                                                                         *
#*   This program is distributed in the hope that it will be useful,       *
#*   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
#*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
#*   GNU General Public License for more details.                          *
#***************************************************************************

import bz2
import fcntl
import os
import re
import stat
import time

from lib import *



BACKUP_TEMP_DIR_NAME = ".temp"
BACKUP_LOCKFILE_NAME = ".lock"

BACKUP_GROUP_NAME_RE = re.compile("""^\d{4}\.\d{2}\.\d{2}_\d{2}\.\d{2}.\d{2}_-_\d{4}\.\d{2}\.\d{2}_\d{2}\.\d{2}.\d{2}$|^current$""")
BACKUP_NAME_RE = re.compile("^\d{4}\.\d{2}\.\d{2}_\d{2}\.\d{2}\.\d{2}$")

BACKUP_GROUP_NAME_FORMAT = "%s_-_%s"
BACKUP_NAME_STRFTIME_FORMAT = "%Y.%m.%d_%H.%M.%S"
BACKUP_EXTERN_FILES_FILE_NAME = "extern_files.bz2"
BACKUP_UNIQUE_FILES_FILE_NAME = "unique_files.bz2"
BACKUP_LOG_FILE_NAME = "log.bz2"

UNIQUE_FILE_LINE_RE = re.compile("""^([a-f0-9]+)\s+(.+)$""")
EXTERN_FILE_LINE_RE = re.compile("""^([a-f0-9]+)\s+(.+)$""")

EXCEPTION_FILE_NAMES_IN_GROUPS_LIST = (BACKUP_TEMP_DIR_NAME, BACKUP_LOCKFILE_NAME)



class Backup_fs_error(Exception):
	pass



class Backup_fs:

	temp_current_group_path = ""
	temp_current_backup_path = ""



	def __init__(self, config):
		self.config = config

		if self.open():
			raise Backup_fs_error



	def __del__(self):
		if self.config.mode == "b" and self.is_locked():
			self.remove_backup_temp_dir()

		if self.is_locked():
			self.unlock()



	def add_backup_unique_files_to_dictionary(self, group, backup, dictionary):

		D(_("Getting unique files list for backup '%s' in group '%s'...") % (backup, group))

		file_path = os.path.join(self.get_backup_path(group, backup), BACKUP_UNIQUE_FILES_FILE_NAME)

		try:
			old_line = None

			for line in bz2.BZ2File(file_path, "r"):
				if old_line != None:
					matches = UNIQUE_FILE_LINE_RE.search(old_line)
					if matches:
						unique_file_hash = matches.group(1)
						unique_file = matches.group(2)

						dictionary[unique_file_hash] = backup + ":" + unique_file
					else:
						E(_("Bad unique files list line in '%s': '%s'.") % (file_path, old_line))

				old_line = line

			if old_line == None or not BLANK_LINE_RE.search(old_line):
				E(_("Unique files list '%s' is broken! It is not ending by empty line. Skiping it's last line.") % file_path)
		except IOError, e:
			E(_("Can't read unique files list '%s': %s.") % (file_path, EE(e)))
			return 1

		return 0



	def check_backup_root(self):

		# Проверяем, действительно ли нам передали путь к бэкапу -->
		if self.config.mode == "r":
			if not os.path.exists(self.config.restore.src_path):
				E(_("Bad backup '%s': it is not exists.") % self.config.restore.src_path)
				return 1

			if not os.path.isdir(self.config.restore.src_path):
				E(_("Bad backup '%s': it is not a directory.") % self.config.restore.src_path)
				return 1

			if		len(self.config.restore.src_backup.splitlines()) != 1 or \
					not BACKUP_NAME_RE.search(self.config.restore.src_backup) or \
					len(self.config.restore.src_group.splitlines()) != 1 or \
					not BACKUP_GROUP_NAME_RE.search(self.config.restore.src_group):
				E(_("'%s' is not a backup root.") % self.config.backup_root)
				return 1
		# Проверяем, действительно ли нам передали путь к бэкапу <--

		try:
			if is_exists(self.config.backup_root):
				if not is_dir(self.config.backup_root):
					E(_("Bad backup root: '%s' is not a directory.") % self.config.backup_root)
					return 1
			else:
				if self.config.mode == "b":
					I(_("Backup root directory is not exists. Creating it..."))

					if mkdir(self.config.backup_root):
						E(_("Creating backup root directory failed."))
						return 1
				else:
					E(_("Backup root directory '%s' is not exists.") % self.config.backup_root)
					return 1
		except Path_error:
			E(_("Bad backup root directory '%s'.") % self.config.backup_root)
			return 1

		return 0



	def close(self):
		"""Перемещает бэкап из временного хранилища в постоянное"""

		try:
			# Перемещаем только что созданный бэкап -->
			if rename(self.get_backup_path("current", "current"), self.get_backup_path("current", time.strftime(BACKUP_NAME_STRFTIME_FORMAT, time.localtime()))):
				raise Function_error

			self.temp_current_backup_path = ""
			# Перемещаем только что созданный бэкап <--

			# Если нужно переместить еще и группу -->
			if self.temp_current_group_path:
				temp_current_group_path = self.temp_current_group_path
				self.temp_current_group_path = ""

				if rename(temp_current_group_path, self.get_group_path("current")):
					raise Function_error
			# Если нужно переместить еще и группу <--

			# Если при формировании current группы обнаружится, что она битая, то
			# только что созданный нами бэкап будет удален вместе с ней.
			if self.finalize_current_group_if_needed():
				raise Function_error

			# Если старые группы не удалятся - ничего особо страшного в этом не будет
			self.remove_old_groups_if_needed()
		except Function_error:
			E(_("Finalizing current backup failed."))
			return 1

		return 0



	def create_current_backup_dir(self):
		try:
			if is_exists(self.get_group_path("current")):
				# Создаем временную директорию для текущего бэкапа -->
				self.temp_current_backup_path = mkdtemp(self.get_backup_temp_path())
				if not self.temp_current_backup_path:
					raise Function_error
				# Создаем временную директорию для текущего бэкапа <--
			else:
				# Создаем временную current группу -->
				self.temp_current_group_path = mkdtemp(self.get_backup_temp_path())
				if not self.temp_current_group_path:
					raise Function_error
				# Создаем временную current группу <--

				# Создаем временную директорию для текущего бэкапа -->
				current_backup_path = self.get_backup_path("current", "current")
				if mkdir(current_backup_path):
					raise Function_error
				# Создаем временную директорию для текущего бэкапа <--
		except (Function_error, Path_error):
			E(_("Creating current backup directory failed."))
			return 1

		return 0



	def create_backup_temp_dir(self):

		# Удаляем старую, если она осталась от прошлого бэкапа
		self.remove_backup_temp_dir()

		temp_dir = self.get_backup_temp_path()

		try:
			if is_exists(temp_dir):
				if not is_dir(temp_dir):
					E(_("Bad backup temp directory: '%s' is not a directory.") % temp_dir)
					return 1
			else:
				if mkdir(temp_dir):
					raise Function_error
		except (Path_error, Function_error):
			E(_("Creating backup temp directory '%s' failed.") % temp_dir)
			return 1

		return 0



	def finalize_current_group_if_needed(self):
		"""Завершает формирование current группы, если для этого пришло время."""

		backups = self.get_group_backups("current")

		# Если current группа повреждена, то удаляем ее.
		if not backups:
			E(_("Current group is broken! Removing it..."))
			self.remove_group("current")
			return 1

		# Количество бэкапов в группе не ограничено
		if self.config.backup.backups_per_group == 0:
			return 0

		# Если настало время завершить формирование группы -->
		if len(backups) >= self.config.backup.backups_per_group:
			new_group_name = BACKUP_GROUP_NAME_FORMAT % (backups[0], backups[len(backups) - 1])

			I(_("Finalizing current group to '%s'...") % new_group_name)
			if rename(self.get_group_path("current"), self.get_group_path(new_group_name)):
				E(_("Finalizing current group failed."))
		# Если настало время завершить формирование группы <--

		return 0



	def get_backup_extern_files(self, group, backup):

		extern_files = {}
		file_path = os.path.join(self.get_backup_path(group, backup), BACKUP_EXTERN_FILES_FILE_NAME)

		try:
			old_line = None

			for line in bz2.BZ2File(file_path, "r"):
				if old_line != None:
					matches = EXTERN_FILE_LINE_RE.search(old_line)
					if matches:
						extern_file_hash = matches.group(1)
						extern_file_path = matches.group(2)

						extern_files[extern_file_path] = extern_file_hash
					else:
						E(_("Bad extern files list line in '%s': '%s'.") % (file_path, old_line))

				old_line = line

			if old_line == None or not BLANK_LINE_RE.search(old_line):
				E(_("Extern files list '%s' is broken! It is not ending by empty line. Skiping it's last line.") % file_path)
		except IOError, e:
			E(_("Can't read extern files list '%s': %s.") % (file_path, EE(e)))
			return None

		return extern_files



	def get_backup_path(self, group, backup):
		if backup == "current" and self.temp_current_backup_path:
			return self.temp_current_backup_path
		else:
			return os.path.join(self.get_group_path(group), backup)



	def get_backup_temp_path(self):
		return os.path.join(self.config.backup_root, BACKUP_TEMP_DIR_NAME)



	def get_group_backups(self, group):

		group_path = self.get_group_path(group)

		try:
			backups_list = os.listdir(group_path)
		except OSError, e:
			E(_("Can't read group '%s' directory '%s': %s.") % (group, group_path, EE(e)))
			E(_("Getting group '%s' backups list failed.") % group)
			return None


		backups = []

		for backup in backups_list:
			if len(backup.splitlines()) == 1 and BACKUP_NAME_RE.search(backup) and os.path.isdir(os.path.join(group_path, backup)):
				backups.append(backup)
			elif backup == "current":
				pass
			else:
				E(_("Bad backup '%s' in group '%s'.") % (backup, group))

		return backups



	def get_group_path(self, group):
		if group == "current" and self.temp_current_group_path:
			return self.temp_current_group_path
		else:
			return os.path.join(self.config.backup_root, group)



	def get_group_unique_files(self, group):
		backups = self.get_group_backups(group)
		if backups == None:
			E(_("Getting group '%s' unique files list failed.") % group)
			return None

		unique_files = {}
		for backup in backups:
			self.add_backup_unique_files_to_dictionary(group, backup, unique_files)
		return unique_files



	def get_lockfile_path(self):
		return os.path.join(self.config.backup_root, BACKUP_LOCKFILE_NAME)



	def is_locked(self):
		return hasattr(self, "lockfile")



	def lock(self):

		D(_("Trying to lock backup root..."))

		lockfile_path = self.get_lockfile_path()

		# Открываем lockfile -->
		try:
			lockfile_fd = None
			lockfile_fd = os.open(lockfile_path, os.O_RDONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_LARGEFILE, FILES_MODE)
			lockfile = os.fdopen(lockfile_fd, "r")
		except (OSError, ValueError, IOError), e:
			E(_("Can't lock backup root '%s': can't create (open) lock file '%s' (%s).") % (self.config.backup_root, lockfile_path, EE(e)))

			if lockfile_fd != None:
				try:
					os.close(lockfile_fd)
				except OSError:
					pass

			return 1
		# Открываем lockfile <--

		try:
			try:
				fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
			except IOError, e:
				D(_("Can't lock file '%s': %s.") % (lockfile_path, EE(e)))
				raise Function_error

			# Нам удалось заблокировать lock файл. Это означает что:
			# - Мы первые успели заблокировать файл.
			# или
			# - Кто-то только что снял блокировку с файла, и мы сразу же ее
			#   захватили. В таком случае предыдущий владелец блокировки перед
			#   ее снятием выполнил unlink и удалил этот файл.
			# Поэтому:
			# -->
			try:
				test_file = open(lockfile_path)
			except IOError:
				# Мы захватили блокировку уже несуществующего файла. Не будем
				# дальше включаться в гонку за ресурсами, а просто сделаем вид,
				# что мы попытались заблокировать lockfile чуть раньше, когда
				# им еще владел другой процесс.
				raise Function_error

			if os.path.sameopenfile(lockfile.fileno(), test_file.fileno()):
				# Файл остался тот же. Это значит, что мы теперь являемся
				# владельцем блокировки.
				pass
			else:
				# Мы захватили блокировку уже несуществующего файла. Не будем
				# дальше включаться в гонку за ресурсами, а просто сделаем вид,
				# что мы попытались заблокировать lockfile чуть раньше, когда
				# им еще владел другой процесс.
				raise Function_error
			# <--
		except Function_error:
			E(_("Can't lock backup root '%s': backing up or restoring is already started by another process.") % self.config.backup_root)
			return 1

		# Таким образом файл останется открытым до момента уничтожения
		# класса, а, следовательно, блокировка также будет действовать
		# все это время.
		self.lockfile = lockfile

		D(_("Backup root locked."))

		return 0



	def open(self):
		if self.check_backup_root():
			return 1

		if self.lock():
			return 1

		if self.config.mode == "b":
			if self.create_backup_temp_dir():
				return 1

			if self.create_current_backup_dir():
				return 1

		return 0



	def remove_backup_temp_dir(self):
		if rm_if_exists(self.get_backup_temp_path()):
			E(_("Removing backup temp directory '%s' failed.") % self.get_backup_temp_path())
			return 1
		return 0



	def remove_group(self, group):

		I(_("Removing group '%s'...") % group )

		try:
			trash_path = mkdtemp(self.get_backup_temp_path())
			if not trash_path:
				raise Function_error

			group_path = self.get_group_path(group)
			group_trash_path = os.path.join(trash_path, group)

			if rename(group_path, group_trash_path):
				raise Function_error

			rm(trash_path)
		except Function_error:
			E(_("Removing group '%s' failed.") % group)
			return 1

		return 0



	def remove_old_groups_if_needed(self):

		try:
			# Количество групп не ограничено
			if self.config.backup.groups_per_backup_root == 0:
				return 0

			valid_groups = []

			try:
				groups_list = os.listdir(self.config.backup_root)
			except OSError, e:
				E(_("Can't read backup root directory '%s': %s.") % (self.config.backup_root, EE(e)))
				raise Function_error
			else:
				# Чтобы не посчитать за группу какой-нибудь
				# случайно попавший сюда файл.
				# -->
				for group in groups_list:
					if len(group.splitlines()) == 1 and BACKUP_GROUP_NAME_RE.search(group) and is_dir(os.path.join(self.config.backup_root, group)):
						valid_groups.append(group)
					elif group in EXCEPTION_FILE_NAMES_IN_GROUPS_LIST:
						pass
					else:
						E(_("Bad group '%s' in backup root directory '%s'.") % (group, self.config.backup_root))
				# <--

				# Сортируем список, чтобы группы в нем располагались
				# по времени их создания
				valid_groups.sort()

				D(_("Gotten backup groups list: %s.") % valid_groups)

				# Удаляем все старые группы -->
				is_error = 0

				if range(0, len(valid_groups) - self.config.backup.groups_per_backup_root):
					I(_("Removing old groups..."))

				for i in range(0, len(valid_groups) - self.config.backup.groups_per_backup_root):
					is_error |= self.remove_group(valid_groups[i])

				if is_error:
					raise Function_error
				# Удаляем все старые группы <--
		except (Function_error, Path_error):
			E(_("Removing old groups failed."))
			return 1

		return 0



	def unlock(self):
		unlink(self.get_lockfile_path())
		del self.lockfile



	def write_backup_extern_files(self, extern_files):
		file_path = os.path.join(self.get_backup_path("current", "current"), BACKUP_EXTERN_FILES_FILE_NAME)

		try:
			fd = bz2.BZ2File(file_path, "w")

			# Никакой конфиденциальной информации данный файл не несет,
			# так что можно позволить себе не отслеживать ошибки в изменении
			# прав доступа (к тому же права на родительскую директорию все
			# равно предотвратят доступ к нему).
			chmod(file_path, FILES_MODE)

			for extern_file_hash, extern_file_path in extern_files:
				fd.write(extern_file_hash + " " + extern_file_path + "\n")

			fd.write("\n")
		except IOError, e:
			E(_("Can't write backup extern files info to '%s': %s.") % (file_path, EE(e)))
			return 1

		return 0



	def write_backup_unique_files(self, unique_files):
		file_path = os.path.join(self.get_backup_path("current", "current"), BACKUP_UNIQUE_FILES_FILE_NAME)

		try:
			fd = bz2.BZ2File(file_path, "w")

			# Никакой конфиденциальной информации данный файл не несет,
			# так что можно позволить себе не отслеживать ошибки в изменении
			# прав доступа (к тому же права на родительскую директорию все
			# равно предотвратят доступ к нему).
			chmod(file_path, FILES_MODE)

			for unique_file_hash, unique_file_path in unique_files:
				fd.write(unique_file_hash + " " + unique_file_path + "\n")

			fd.write("\n")
		except IOError, e:
			E(_("Can't write backup unique files info to '%s': %s.") % (file_path, EE(e)))
			return 1

		return 0



	def write_backup_log(self, log):
		file_path = os.path.join(self.get_backup_path("current", "current"), BACKUP_LOG_FILE_NAME)

		try:
			fd = bz2.BZ2File(file_path, "w")
			# Никакой конфиденциальной информации данный файл не несет,
			# так что можно позволить себе не отслеживать ошибки в изменении
			# прав доступа (к тому же права на родительскую директорию все
			# равно предотвратят доступ к нему).
			chmod(file_path, FILES_MODE)
			fd.write(log.get())
		except IOError, e:
			E(_("Can't write backup log file '%s': %s.") % (file_path, EE(e)))
			return 1

		return 0

