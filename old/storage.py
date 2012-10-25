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

import os
import tarfile

from lib import *



TAR_FILE_NAME = "data.tar"



class Tar_storage:

	__tar_fd = None
	__members_list = None
	__members = None



	def __del__(self):
		if self.is_opened():
			Tar_storage.close(self)



	def add(self, file_info, path = None):
		try:
			if file_info.isfile():
				if not path:
					E(_("Logical error!"))
					return 1

				self.__tar_fd.addfile(file_info, open(path, "r"))
			else:
				path = "/" + file_info.name
				self.__tar_fd.addfile(file_info)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			E(_("Can't add '%s' to tar file '%s': %s.") % (path, self.__tar_path, EE(e)))
			return 1

		return 0



	def add_opened_file(self, file_info, file):
		try:
			self.__tar_fd.addfile(file_info, file)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			E(_("Can't add file to tar file '%s': %s.") % (self.__tar_path, EE(e)))
			return 1

		return 0



	def check_tarinfo(self, tarinfo, path):
		"""
		Т. к. содержимое файлов-жестких ссылок не добавляется в tar архив, то из каждой жесткой
		ссылки мы делаем обычный файл (это никак не повлияет на работу библиотеки tarfile - я
		смотрел ее исходный код, так что тут можно быть спокойным).

		Т. к. gettarinfo для файлов-сокетов возвращает None, то выдаем предупреждение пользователю.
		"""

		if not tarinfo:
			W(_("File type of '%s' is not supported.") % path)
		elif tarinfo.islnk():
			tarinfo.type = tarfile.REGTYPE
			tarinfo.link_name = ""

		return tarinfo



	def close(self):
		if self.is_opened():
			try:
				self.__tar_fd.close()
			except (OSError, IOError, tarfile.TarError), e:
				E(_("Can't close tar file '%s': %s.") % (self.__tar_path, EE(e)))
				self.__tar_fd = None
				return 1

			self.__tar_fd = None

			return 0
		else:
			return 1



	def create_members_cache(self):
		"""
		Функция getmember производит линейный поиск нужного TarInfo объекта.
		Это _очень_ замедляет процесс восстановления бэкапа, который активно использует
		данную функцию. Поэтому делаем словарь для ускорения поиска нужных нам TarInfo.
		"""

		D(_("Creating tar file '%s' members cache...") % self.__tar_path)

		if self.__members_list == None:
			members_list = self.__tar_fd.getmembers()
		else:
			# Если список файлов tar архива мы храним у себя в памяти
			members_list = self.__members_list

		self.__members = {}

		for member in members_list:
			self.__members[member.name] = member

		D(_("Members cache has been created."))



	def extract_all(self, path):
		try:
			self.__tar_fd.extractall(path)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			E(_("Can't extract files from tar file '%s' to '%s': %s.") % (self.__tar_path, path, EE(e)))
			return 1

		return 0



	def get_file_info(self, path):
		try:
			return self.check_tarinfo(self.__tar_fd.gettarinfo(path, path[1:]), path)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			W(_("Can't get file '%s' info: %s.") % (path, EE(e)))

		return None



	def get_member_file(self, file_info):
		return self.__tar_fd.extractfile(file_info)



	def get_member_info(self, path):
		if self.__members == None:
			self.create_members_cache()

		try:
			return self.__members[path[1:]]
		except KeyError:
			return None



	def get_members(self):
		"""
		Эту функцию будут использовать только внешние классы.
		Получение списка всех файлов архива - очень ресурсоемкая операция,
		поэтому, если внешний класс хотя бы один раз запросит этот список,
		кэшируем его и оставляем в памяти.
		Это имеет смысл также потому, что функция create_members_cache
		нуждается в данном списке, а т. к. внешние классы вызывают эту
		функцию до того, как будет выполнена create_members_cache, мы
		экономим достаточно приличное количество времени, если tar архив
		содержит очень большое количество файлов.
		"""

		if self.__members_list == None:
			D(_("Getting tar file '%s' members list...") % self.__tar_path)
			self.__members_list = self.__tar_fd.getmembers()
			D(_("Members list has been gotten."))

		return self.__members_list



	def get_opened_file_info(self, file, path):
		try:
			return self.check_tarinfo(self.__tar_fd.gettarinfo(fileobj = file, arcname = path[1:]), path)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			W(_("Can't get file '%s' info: %s.") % (path, EE(e)))

		return None



	def is_opened(self):
		if self.__tar_fd:
			return 1
		else:
			return 0



	def open(self, path, mode):

		self.__tar_path = path
		self.__mode = mode

		D(_("Opening tar file '%s' with mode '%s'...") % (self.__tar_path, self.__mode))

		try:
			self.__tar_fd = tarfile.open(self.__tar_path, self.__mode)
		except (OSError, IOError, tarfile.TarError, ValueError), e:
			E(_("Can't open tar file '%s': %s.") % (self.__tar_path, EE(e)))
			return 1

		D(_("Tar file has been opened."))

		return 0



class Storage_error(Exception):
	pass



class Storage(Tar_storage):

	# Режимы, с которыми можно открывать storage -->
	mode_read = 1
	mode_write = 2
	mode_temp = 4
	# Режимы, с которыми можно открывать storage <--



	def __init__(self, config, fs, mode, group = "", backup = ""):

		self.config = config
		self.fs = fs
		self.mode = mode

		if self.mode not in (Storage.mode_read, Storage.mode_write, Storage.mode_temp):
			E(_("Logical error!"))
			raise Storage_error

		if self.mode in (Storage.mode_read, Storage.mode_write):
			if not group or not backup:
				E(_("Logical error!"))
				raise Storage_error

			self.backup_path = self.fs.get_backup_path(group, backup)

		if self.mode == Storage.mode_read:
			# Ищем архив в папке с бэкапом -->
			D(_("Finding archive with backup data in '%s'...") % self.backup_path)

			storage_path = ""

			for file_name in (TAR_FILE_NAME, TAR_FILE_NAME + ".7z", TAR_FILE_NAME + ".bz2", TAR_FILE_NAME + ".gz"):
				path = os.path.join(self.backup_path, file_name)
				try:
					if is_exists(path):
						storage_path = path
						break
				except Path_error:
					E(_("Opening backup '%s' data archive failed.") % self.backup_path)
					raise Storage_error

			if not storage_path:
				E(_("Can't find archive with backup data in '%s'.") % self.backup_path)
				raise Storage_error

			D(_("Archive found: '%s'.") % storage_path)
			# Ищем архив в папке с бэкапом <--

			# Тип архива
			self.type = storage_path[storage_path.rindex(".") + 1:]
			D(_("Archive type: %s.") % self.type)
		elif self.mode == Storage.mode_write:
			self.type = self.config.backup.format
		elif self.mode == Storage.mode_temp:
			self.type = "tar"

		# Создаем временную директорию -->
		self.temp_dir_path = mkdtemp()
		if not self.temp_dir_path:
			E(_("Creating tar storage temp directory failed."))
			raise Storage_error
		# Создаем временную директорию <--

		if self.open():
			raise Storage_error



	def __del__(self):
		Tar_storage.__del__(self)

		# Удаляем временную директорию
		if hasattr(self, "temp_dir_path") and self.temp_dir_path:
			rm(self.temp_dir_path)



	def close(self, extract_to_path = ""):
		"""
		Если указан путь extract_to_path, и архив имеет временный тип,
		то после закрытия распаковывает весь архив в этот каталог.
		"""

		if extract_to_path and self.mode != Storage.mode_temp:
			E(_("Logical error!"))
			return 1

		if not self.is_opened():
			E(_("Logical error!"))
			return 1

		# Если архив пуст, то tarfile не сможет потом его открыть.
		# Поэтому...
		# -->
		if extract_to_path:
			if self.get_members():
				is_archive_empty = 0
			else:
				is_archive_empty = 1
		# <--

		if Tar_storage.close(self):
			return 1

		# Распаковываем получившийся архив, если это необходимо -->
		if extract_to_path and not is_archive_empty:
			extract_archive_path = self.get_temp_archive_path()

			try:
				tar_fd = tarfile.open(extract_archive_path, "r")
				tar_fd.extractall(extract_to_path)
				tar_fd.close()
			except (OSError, IOError, tarfile.TarError, ValueError), e:
				E(_("Can't extract tar archive '%s' to '%s': %s.") % (extract_archive_path, extract_to_path, EE(e)))
				return 1
		# Распаковываем получившийся архив, если это необходимо <--

		# Запаковываем получившийся архив, если это необходимо -->
		if self.mode == Storage.mode_write and self.type == "7z":
			I(_("Compressing backup data archive..."))

			if self.convert_tar_to_7z():
				# Если создать 7z архив не удалось, будем использовать
				# вместо него уже существующий tar архив.
				if move(self.get_temp_archive_path(), os.path.splitext(self.get_archive_path())[0]):
					unlink_if_exists(self.get_archive_path)
					E(_("Creating archive '%s' with backup data failed.") % self.get_archive_path())
					return 1
				else:
					I(_("Tar archive will be used instead of 7z archive."))
		# Запаковываем получившийся архив, если это необходимо <--

		return 0



	def convert_7z_to_tar(self):

		archive_path = self.get_archive_path()

		D(_("Uncompressing 7z archive '%s'...") % archive_path)

		# 7z распаковывает файл с такими правами, с которыми он был запакован
		try:
			exit_status, shell_output = run_command( "7z -y -o'%s' x '%s' '%s'" % (self.temp_dir_path.replace("'", """'"'"'"""), archive_path.replace("'", """'"'"'"""), TAR_FILE_NAME) )
			if exit_status:
				E(_("Uncompressing 7z archive with backup data '%s' to '%s' failed. Shell output:\n%s") % (archive_path, self.temp_dir_path, shell_output))
				return 1
		except Error:
			E(_("Uncompressing 7z archive with backup data '%s' failed.") % archive_path)
			return 1

		D(_("Uncompressing 7z archive has been finished successfully."))

		return 0



	def convert_bz2_or_gz_to_tar(self):

		if decompress_file(self.get_archive_path(), self.get_temp_archive_path()):
			E(_("Decompressing archive with backup data '%s' to '%s' failed.") % (self.get_archive_path(), self.get_temp_archive_path()))
			return 1

		return 0



	def convert_tar_to_7z(self):

		archive_path = self.get_archive_path()
		temp_archive_path = self.get_temp_archive_path()

		D(_("Compressing tar archive '%s' to 7z...") % temp_archive_path)

		# 7z создает файл с минимальными правами
		try:
			exit_status, shell_output = run_command( "7z -y a '%s' '%s' -mx=9" % (archive_path.replace("'", """'"'"'"""), temp_archive_path.replace("'", """'"'"'""")) )
			if exit_status:
				E(_("Compressing archive with backup data '%s' to 7z failed. Shell output:\n%s") % (temp_archive_path, shell_output))
				return 1
		except Error:
			E(_("Compressing archive with backup data '%s' to 7z failed.") % temp_archive_path)
			return 1

		D(_("Compressing archive to 7z has been finished successfully."))

		return 0



	def get_temp_archive_path(self):
		return os.path.join(self.temp_dir_path, TAR_FILE_NAME)



	def get_archive_path(self):
		path = os.path.join(self.backup_path, TAR_FILE_NAME)

		if self.type != "tar":
			path += "." + self.type

		return path



	def make_temp_tar_copy_of_archive(self):

		I(_("Uncompressing archive with backup data '%s'...") % self.get_archive_path())

		try:
			if self.type in ("bz2", "gz"):
				if self.convert_bz2_or_gz_to_tar():
					raise Function_error
			elif self.type == "7z":
				if self.convert_7z_to_tar():
					raise Function_error
			else:
				E(_("Logical error!"))
				raise Function_error
		except Function_error:
			return 1

		I(_("Uncompressing archive finished."))

		return 0



	def open(self):

		# Преобразовываем сжатый архив в обычный tar
		# для временного использования, если это необходимо.
		if self.mode == Storage.mode_read and self.type != "tar":
			if self.make_temp_tar_copy_of_archive():
				return 1

		# Режим открытия tar файла -->
		if self.mode == Storage.mode_read:
			mode = "r"
		elif self.mode == Storage.mode_write:
			mode = "w"

			if self.type in ("bz2", "gz"):
				mode += ":" + self.type
		elif self.mode == Storage.mode_temp:
			mode = "w"
		# Режим открытия tar файла <--

		# Определяем путь архива -->
		if self.mode == Storage.mode_temp or ( self.mode == Storage.mode_write and self.type == "7z" ) or ( self.mode == Storage.mode_read and self.type != "tar" ):
			archive_path = self.get_temp_archive_path()
		else:
			archive_path = self.get_archive_path()
		# Определяем путь архива <--

		# Создаем файл с теми правами доступа, которые требуют правила безопасности -->
		if self.mode == Storage.mode_temp or self.mode == Storage.mode_write:
			try:
				os.close(os.open(archive_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_LARGEFILE, FILES_MODE))
			except (OSError, IOError), e:
				E(_("Can't create tar file '%s': %s.") % (archive_path, EE(e)))
				return 1
		# Создаем файл с теми правами доступа, которые требуют правила безопасности <--

		if Tar_storage.open(self, archive_path, mode):
			return 1

