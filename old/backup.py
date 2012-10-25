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

import hashlib
import os
import tarfile
import time

from lib import *
from backup_fs import Backup_fs, Backup_fs_error
from config import Config, Config_error
from storage import Storage, Storage_error



class Dir_tree_error(Exception):
	pass



class Backup_error(Exception):
	pass



class Backup:

	def __init__(self, config_path):

		# Начинаем запись лога -->
		self.log = String_log()
		self.log_errors_counter = Log_errors_counter()
		# Начинаем запись лога <--

		try:
			self.config = Config("b", config_path)
		except Config_error:
			raise Backup_error

		try:
			self.fs = Backup_fs(self.config)
		except Backup_fs_error:
			raise Backup_error

		# Списки файлов текущего и других бэкапов -->
		self.avalible_extern_files = self.fs.get_group_unique_files("current")
		if self.avalible_extern_files == None:
			self.avalible_extern_files = {}

		self.added_files = {}
		self.unique_files = []
		self.extern_files = []
		# Списки файлов текущего и других бэкапов <--

		# Создаем временный файл для добавления в архив простых файлов -->
		self.temp_storage_file_path = mkstemp()
		if not self.temp_storage_file_path:
			raise Backup_error
		# Создаем временный файл для добавления в архив простых файлов <--

		# Создаем хранилище для бэкапа -->
		try:
			self.storage = Storage(self.config, self.fs, Storage.mode_write, "current", "current")
		except Storage_error:
			E(_("Creating tar archive for current backup failed."))
			raise Backup_error
		# Создаем хранилище для бэкапа <--

		I(_("Backup started with configuration file '%s' at %s.") % (config_path, time.strftime("%H:%M:%S %d.%m.%Y", time.localtime())))

		if self.create():
			raise Backup_error

		# Рапортуем об окончании бэкапа -->
		errors = self.log_errors_counter.get()

		if errors[1]:
			I(_("Backup completed with errors at %s.") % time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))
		elif errors[0]:
			I(_("Backup completed with warnings at %s.") % time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))
		else:
			I(_("Backup successfully completed at %s.") % time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))
		# Рапортуем об окончании бэкапа <--



	def __del__(self):

		if hasattr(self, "temp_storage_file_path") and self.temp_storage_file_path:
			unlink(self.temp_storage_file_path)

		if hasattr(self, "config"):
			self.send_email_report_if_needed()



	def backup_object(self, backup_entry, rel_path, call_count = 1):

		path = os.path.join(backup_entry.path, rel_path)

		if call_count >= MAX_RECURSION_DEPTH:
			E(_("Maximum recursion depth exceeded while backing up file '%s'.") % path)
			return 1

		# Если backup_entry.name == "/", то backup_entry.path == "/", а rel_path == ""
		if rel_path:
			# Один и тот же путь не бэкапим несколько раз
			if self.added_files.has_key(path):
				D(_("Skipping '%s' - it is already treated.") % path)
				return 0

			# Накладываем на файл фильтры -->
			is_match = 0

			for path_filter in backup_entry.filters:
				if path_filter.re.search(rel_path):
					is_match = 1

					D(_("'%s' is matches filter '%s'.") % (path, path_filter.string))

					if path_filter.policy == "-":
						D(_("Skipping '%s' - it has been filtered.") % path)
						return 0

					break

			if not is_match:
				if backup_entry.filter_default_policy == "-":
					D(_("Skipping '%s' - it has been filtered by default filter.") % path)
					return 0
			# Накладываем на файл фильтры <--

		try:
			D(_("Backing up '%s'...") % path)

			file_info = self.storage.get_file_info(path)
			if not file_info:
				raise Function_error


			if file_info.isfile():

				try:
					# Сохраняем файл во временное хранилище и получаем
					# информацию о нем.
					file_hash, file_info, temp_path = self.save_file_for_storage(path)
				except Error:
					raise Function_error

				if self.avalible_extern_files.has_key(file_hash):
					# Этот файл мы можем позаимствовать из текущего
					# или другого бэкапа.

					# Присваиваем файлу тип "жесткая ссылка", тем самым помечая его
					# как файл, которому необходим внешний файл.
					file_info.type = tarfile.LNKTYPE
					file_info.linkname = ""

					# Добавляем информацию о файле в архив
					if self.storage.add(file_info):
						raise Function_error

					self.extern_files.append( (file_hash, path) )
				else:
					# Такого файла мы еще не знаем.

					# Добавляем файл в архив
					if self.storage.add(file_info, temp_path):
						raise Function_error

					# Добавляем файл в список известных нам файлов.
					self.avalible_extern_files[file_hash] = ":" + path

					self.unique_files.append( (file_hash, path) )

				# Отмечаем, что данный путь мы уже обработали
				self.added_files[path] = file_info


			elif file_info.isdir():

				# Если backup_entry.name == "/", то backup_entry.path == "/", а rel_path == ""
				if rel_path:
					# Добавляем директорию в архив
					if self.storage.add(file_info):
						raise Function_error

					# Отмечаем, что данный путь мы уже обработали
					self.added_files[path] = file_info

				# Бэкапим все файлы этой директории -->
				try:
					for dir_file in os.listdir(path):
						self.backup_object(backup_entry, os.path.join(rel_path, dir_file), call_count + 1)
				except OSError, e:
					W(_("Can't read directory '%s': %s.") % (path, EE(e)))
					raise Function_error
				# Бэкапим все файлы этой директории <--


			else:
				# Остальные специальные файлы: символические ссылки, FIFO, файлы устройств...

				# Добавляем файл в архив
				if self.storage.add(file_info):
					raise Function_error

				# Отмечаем, что данный путь мы уже обработали
				self.added_files[path] = file_info

		except Function_error:
			W(_("Backing up of '%s' failed.") % path)
			return 1

		return 0



	def create(self):

		self.process_entries()

		if not self.added_files:
			# Во-первых, это сделано для того, чтобы битые бэкапы не заполняли группу, но основная
			# причина состоит в том, что библиотека tarfile не позволяет открывать на чтение архивы,
			# в которых нет ни одного файла.
			E(_("No files added to backup archive. Backup is empty. Not adding it to the current group."))
			return 1

		if self.storage.close():
			E(_("Creating backup data archive failed."))
			return 1

		# Пишем информационные файлы.
		# Если какой-то файл не удастся записать, то это, кончено, серьезная
		# потеря, но не смертельная. Все-таки все самое важное (новые файлы)
		# хранится в tar архиве.
		# -->
		# Для лога, который будет храниться в папке с бэкапом -->
		if self.fs.write_backup_extern_files(self.extern_files) | self.fs.write_backup_unique_files(self.unique_files):
			I(_("Not all backup files has been written. Backup is not full."))
		else:
			I(_("All backup files has been written."))
		# Для лога, который будет храниться в папке с бэкапом <--

		self.fs.write_backup_log(self.log)
		# <--

		if self.fs.close():
			return 1

		return 0



	def create_empty_dir_tree(self, path, call_count = 1):
		"""
			Создает в архиве все недостающие пустые каталоги и ссылки,
			которые содержатся в пути path, и возвращает реальное местоположение
			path. В случае ошибки генерирует исключение Dir_tree_error.
		"""

		if path == "/":
			return path

		if call_count >= MAX_RECURSION_DEPTH:
			E(_("Maximum recursion depth exceeded while creating empty directory tree."))
			raise Dir_tree_error

		file_info = self.storage.get_file_info(path)
		if not file_info:
			raise Dir_tree_error

		# Проверяем, не обрабатывали ли мы уже этот путь ранее -->
		try:
			treated_file_info = self.added_files[path]
		except KeyError:
			# Данный путь мы еще не обрабатывали
			pass
		else:
			# Данный путь уже сохранен в хранилище

			if \
				( not file_info.isdir() and not file_info.issym() ) or \
				( not treated_file_info.isdir() and not treated_file_info.issym() ):
				# Если файл раньше не был директорией или ссылкой на нее,
				# и мы уже успели его сохранить, то теперь мы уже ничего изменить
				# не сможем, поэтому поступаем так, как будто мы сейчас находимся
				# в том времени, когда записывали его в хранилище.
				# Если же файл раньше был директорией или ссылкой на нее, а теперь
				# стал файлом другого типа, то, значит, путь указан неверно и
				# мы не сможем создать дерево каталогов.
				E(_("Error while creating empty directory tree: '%s' is not directory or link to directory.") % path)
				raise Dir_tree_error

			# Сюда управление доходит только тогда, когда текущий файл - либо
			# директория, либо ссылка, и уже записанный - либо директория, либо
			# ссылка.

			if file_info.isdir() and treated_file_info.isdir():
				# Все необходимые директории уже присутствуют
				return path
			elif file_info.issym() and treated_file_info.isdir():
				# Ссылку мы сохранить не можем, но сделать бэкап
				# директории, на которую она указывает вполне в
				# состоянии.
				W(_("'%s' was a directory, but now it is link to '%s'. Rejecting link creation." % (path, file_info.linkname)))
				return self.create_empty_dir_tree(get_link_abs_path(file_info), call_count + 1)
			elif file_info.isdir() and treated_file_info.issym():
				E(_("'%s' was link to '%s', but now it is a directory. Rejecting directory creation." % (path, treated_file_info.linkname)))
				raise Dir_tree_error
			elif file_info.issym() and treated_file_info.issym():
				if file_info.linkname == treated_file_info.linkname:
					# Ничего не изменилось с тех пор как мы сохраняли эту ссылку
					return self.create_empty_dir_tree(get_link_abs_path(file_info), call_count + 1)
				else:
					# Ссылку мы изменить не можем, но сделать бэкап
					# директории, на которую она указывает вполне в
					# состоянии.
					W(_("'%s' was link to '%s', but now it is link to '%s'. Rejecting link changing." % (path, treated_file_info.linkname, file_info.linkname)))
					return self.create_empty_dir_tree(get_link_abs_path(file_info), call_count + 1)
			else:
				E(_("Logical error!"))
				raise Dir_tree_error
		# Проверяем, не обрабатывали ли мы уже этот путь ранее <--

		# Создаем родительскую директорию, если это необходимо
		parent_real_path = self.create_empty_dir_tree(os.path.dirname(path), call_count + 1)

		# Если после содания родительских директорий оказалось,
		# что данный путь уже обработан, то это означает, что
		# в системе есть закольцованные пути.
		if self.added_files.has_key(path):
			E(_("Cycle paths detected at '%s' while creating empty directory tree.") % path)
			raise Dir_tree_error

		# Получаем реальный путь path
		path = os.path.join(parent_real_path, os.path.basename(path))
		file_info.name = path[1:]

		# Создаем нашу директорию -->
		if file_info.isdir():
			# Это обычная директория
			if self.storage.add(file_info):
				raise Dir_tree_error
		elif file_info.issym():
			# Это не директория, а ссылка. Обрабатываем ссылку.
			path = self.create_empty_dir_tree(get_link_abs_path(file_info), call_count + 1)
			if self.storage.add(file_info):
				raise Dir_tree_error
		else:
			E(_("'%s' is not a directory or a link to directory.") % path)
			raise Dir_tree_error
		# Создаем нашу директорию <--

		# Отмечаем, что данный путь мы уже обработали
		self.added_files[path] = file_info

		return path



	def process_entries(self):

		for entry in self.config.backup.entries:

			# Запускаем start before command -->
			if entry.start_before:
				I(_("Running backup entry '%s' start_before command '%s'...") % (entry.name, entry.start_before))

				try:
					exit_status, shell_output = run_command(entry.start_before)
					if exit_status:
						E(_("Backup entry '%s' start_before command return exit status %d. Shell output:\n%s") % (entry.name, exit_status, shell_output))
				except Error:
					pass
			# Запускаем start before command <--

			I(_("Backing up '%s'...") % entry.name)

			# Создаем родительские директории
			try:
				entry.path = self.create_empty_dir_tree(os.path.dirname(entry.name))
			except Dir_tree_error:
				E(_("Creating empty directory tree for backup entry '%s' failed.") % entry.name)
				E(_("Backup of backup entry '%s' failed.") % entry.name)
			else:
				# Запускаем процесс бэкапа
				if self.backup_object(entry, os.path.basename(entry.name)):
					E(_("Backup of backup entry '%s' failed.") % entry.name)

			# Запускаем start after command -->
			if entry.start_after:
				I(_("Running backup entry '%s' start_after command '%s'...") % (entry.name, entry.start_after))

				try:
					exit_status, shell_output = run_command(entry.start_after)
					if exit_status:
						E(_("Backup entry '%s' start_after command return exit status %d. Shell output:\n%s") % (entry.name, exit_status, shell_output))
				except Error:
					pass
			# Запускаем start after command <--



	def save_file_for_storage(self, src_path):
		"""Возвращает хэш, информацию о файле и путь к сохраненному файлу или генерирует Error в случае ошибки."""

		dest_path = self.temp_storage_file_path

		# Открываем наши файлы -->
		# src_file -->
		try:
			src_file_fd = None
			src_file_fd = os.open(src_path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_LARGEFILE)
			src_file = os.fdopen(src_file_fd, "r")
		except (ValueError, OSError, IOError), e:
			W(_("Can't read file '%s': %s.") % (src_path, EE(e)))

			if src_file_fd != None:
				try:
					os.close(src_file_fd)
				except OSError:
					pass

			raise Error
		# src_file <--

		# dest_file -->
		try:
			dest_file = open(dest_path, "w")
		except IOError, e:
			E(_("Can't open temp file '%s' for writing: %s.") % (dest_path, EE(e)))
			raise Error
		# dest_file <--
		# Открываем наши файлы <--

		# Копируем один файл в другой и получаем информацию о обоих файлах -->
		file_info = self.storage.get_opened_file_info(src_file, src_path)
		if not file_info:
			raise Error

		if not file_info.isfile():
			W(_("File type of '%s' has been changed.") % src_path)
			raise Error

		try:
			# Выбираем хэш функцию на основе размера хэша -->
			if self.config.backup.hash_size == 128:
				file_hash = hashlib.sha1()
			elif self.config.backup.hash_size == 224:
				file_hash = hashlib.sha224()
			elif self.config.backup.hash_size == 256:
				file_hash = hashlib.sha256()
			elif self.config.backup.hash_size == 384:
				file_hash = hashlib.sha384()
			elif self.config.backup.hash_size == 512:
				file_hash = hashlib.sha512()
			else:
				E(_("Logical error!"))
				file_hash = hashlib.sha1()
			# Выбираем хэш функцию на основе размера хэша <--

			while 1:
				data = src_file.read(BUF_SIZE)
				if not data:
					break

				file_hash.update(data)
				dest_file.write(data)

			src_file.close()
			dest_file.close()
		except IOError, e:
			E(_("Error while copying file '%s' to '%s': %s.") % (src_path, dest_path, EE(e)))
			raise Error

		file_hash = file_hash.hexdigest()

		dest_file_info = self.storage.get_file_info(dest_path)
		if not dest_file_info:
			E(_("Getting temp storage file info failed."))
			raise Error
		# Копируем один файл в другой и получаем информацию о обоих файлах <--

		# Сливаем информацию о файлах
		file_info.size = dest_file_info.size

		return (file_hash, file_info, dest_path)



	def send_email_report_if_needed(self):

		if self.config.backup.send_email_report:

			# Формируем тему сообщения -->
			message_subject = "%s backup report" % PROGRAM_NAME

			warnings, errors = self.log_errors_counter.get()

			if errors:
				message_subject += " [E]"
			elif warnings:
				message_subject += " [W]"
			# Формируем тему сообщения <--

			message = self.log.get()

			I(_("Sending email report..."))

			return send_email_message(self.config.backup.mail_program, self.config.backup.mail_from, self.config.backup.mail_to, message_subject, message)

		return 0

