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

from lib import *
from backup_fs import Backup_fs, Backup_fs_error
from config import Config, Config_error
from storage import Storage, Storage_error



class Restore_error(Exception):
	pass



class Restore:

	def __init__(self, backup_path):

		self.log_errors_counter = Log_errors_counter()

		try:
			self.config = Config("r", backup_path)
		except Config_error:
			raise Restore_error

		try:
			self.fs = Backup_fs(self.config)
		except Backup_fs_error:
			raise Restore_error

		self.storages = {}

		if self.run():
			raise Restore_error



	def __del__(self):
		# Т. к. удаление временных архивов может занять довольно длительное время...
		if hasattr(self, "storages") and self.storages:
			I(_("Deleting temporary files..."))



	def add_backup_files_to_storage(self, dest_storage):

		# Получаем всю необходимую информацию о внешних файлах -->
		try:
			extern_files = self.fs.get_backup_extern_files(self.config.restore.src_group, self.config.restore.src_backup)
			if extern_files == None:
				raise Function_error

			unique_files = self.fs.get_group_unique_files(self.config.restore.src_group)
			if unique_files == None:
				raise Function_error
		except Function_error:
			extern_files = unique_files = None
			E(_("Restoring of backup extern files will be impossible."))
		# Получаем всю необходимую информацию о внешних файлах <--

		# Открываем хранилище нашего бэкапа -->
		try:
			self.storages[self.config.restore.src_backup] = src_storage = Storage(self.config, self.fs, Storage.mode_read, self.config.restore.src_group, self.config.restore.src_backup)
		except Storage_error:
			E(_("Opening archive with backup data for restoring backup '%s' in group '%s' failed.") % (self.config.restore.src_backup, self.config.restore.src_group))
			return 1
		# Открываем хранилище нашего бэкапа <--

		# Обрабатываем каждый файл бэкапа -->
		for file_info in src_storage.get_members():

			file_path = "/" + file_info.name

			if file_info.isfile():
				# Копируем файл из одного архива в другой
				if dest_storage.add_opened_file(file_info, src_storage.get_member_file(file_info)):
					E(_("Restoring of file '%s' failed.") % (file_path))

			elif file_info.islnk():
				# Жесткие ссылки мы не помещаем в архивы, но типом "жесткая ссылка"
				# отмечаем файлы, которым необходимы внешние файлы.

				if extern_files == None:
					E(_("File '%s' is marked as extern. Skipping restoring of it...") % file_path)
					continue

				try:
					file_hash = extern_files[file_path]
				except KeyError:
					E(_("Backup is broken: file '%s' is link to extern file, but extern files list has not info about it. Skipping restoring of it.") % file_path)
					continue

				try:
					extern_path = unique_files[file_hash]
				except KeyError:
					E(_("Can't find extern file for '%s'. Skipping restoring of it.") % file_path)
					continue

				extern_backup, sep, extern_path = extern_path.partition(":")

				self.add_extern_file_to_storage(extern_backup, extern_path, dest_storage, file_info)

			else:
				# Копируем остальные специальные файлы: директории, ссылки и т. п.
				if dest_storage.add(file_info):
					E(_("Restoring of '%s' failed.") % (file_path))
		# Обрабатываем каждый файл бэкапа <--

		return 0



	def add_extern_file_to_storage(self, src_backup, src_path, dest_storage, dest_info):

		dest_path = "/" + dest_info.name

		D(_("Adding extern file '%s' for '%s' from backup '%s' to temp storage...") % (src_path, dest_path, src_backup))

		try:
			# Открываем хранилище с внешним файлом -->
			try:
				src_storage = self.storages[src_backup]
			except KeyError:
				try:
					self.storages[src_backup] = src_storage = Storage(self.config, self.fs, Storage.mode_read, self.config.restore.src_group, src_backup)
				except Storage_error:
					# Если открыть storage не удалось, то помечаем его,
					# чтобы в будущем не пытаться открыть снова.
					self.storages[src_backup] = None
					E(_("Opening archive with backup data for backup '%s' in group '%s' failed.") % (src_backup, self.config.restore.src_group))

			if src_storage == None:
				E(_("Extern file '%s' for '%s' is located in unavailable archive for backup '%s'.") % (src_path, dest_path, src_backup))
				raise Function_error
			# Открываем хранилище с внешним файлом <--

			# Получаем информацию о внешнем файле -->
			src_info = src_storage.get_member_info(src_path)
			if not src_info:
				E(_("Backup '%s' is broken: it has file '%s', but this file is not exists in backup data archive.") % (src_backup, src_path))
				raise Function_error
			# Получаем информацию о внешнем файле <--

			# На всякий случай -->
			if not src_info.isfile():
				E(_("File '%s' is link to extern file '%s' in backup '%s' which is not a regular file.") % (dest_path, src_path, src_backup))
				raise Function_error
			# На всякий случай <--

			# Формируем из двух информационных структур одну
			dest_info.size = src_info.size
			dest_info.type = src_info.type

			# Копируем файл из одного архива в другой
			if dest_storage.add_opened_file(dest_info, src_storage.get_member_file(src_info)):
				raise Function_error
		except Function_error:
			E(_("Restoring '%s' from extern file failed.") % dest_path)
			return 1

		return 0



	def run(self):

		I(_("Restoring '%s' to '%s'...") % (self.config.restore.src_path, self.config.restore.dest_path))

		# Создаем директорию, в которую будет восстанавливаться бэкап
		if mkdir(self.config.restore.dest_path):
			return 1

		# Создаем временное хранилище -->
		try:
			temp_storage = Storage(self.config, self.fs, Storage.mode_temp)
		except Storage_error:
			E(_("Creating temp tar storage failed."))
			return 1
		# Создаем временное хранилище <--

		I(_("Adding all needed files to temp storage..."))

		# Переносим во временное хранилище все необходимые файлы
		if self.add_backup_files_to_storage(temp_storage):
			return 1

		I(_("Extracting files from temp storage to '%s'...") % self.config.restore.dest_path)

		# Закрываем временное хранилище, попутно распаковывая все
		# записанные в него файлы.
		if temp_storage.close(self.config.restore.dest_path):
			return 1

		# Рапортуем об окончании восстановления -->
		warnings, errors = self.log_errors_counter.get()

		if errors:
			I(_("Restoring completed with errors."))
		elif warnings:
			I(_("Restoring completed with warnings."))
		else:
			I(_("Restoring successfully completed."))
		# Рапортуем об окончании восстановления <--

		return 0

