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
import re
import stat

from lib import *



FILTER_LINE_RE = re.compile("""^([-+#])(.+)$""")
STRIP_START_SPACES_RE = re.compile("""^\s*(.*)$""")



class Backup_config(Struct):

	def __init__(self):

		self.format = "bz2"
		self.backups_per_group = 30
		self.groups_per_backup_root = 0

		self.hash_size = 128

		self.send_email_report = 0
		self.mail_to = ""
		self.mail_from = ""
		self.mail_program = ""

		self.entries = []



class Backup_entry(Struct):

	def __init__(self, name):

		self.name = name
		self.start_before = ""
		self.start_after = ""
		self.filter_default_policy = "+"
		self.filters = []



class Config_error(Exception):
	pass



class Config():

	def __init__(self, mode, path):

		self.mode = mode

		# Значение по умолчанию
		self.backup_root = ""

		if self.mode == "b":
			if self.backup_mode(path):
				raise Config_error
		elif self.mode == "r":
			if self.restore_mode(path):
				raise Config_error
		else:
			E(_("Logical error!"))
			raise Config_error



	def backup_mode(self, config_path):

		config_error_prefix = _("Configuration file error:")

		self.backup = Backup_config()

		# Читаем конфигурационный файл -->
		try:
			from configobj import ConfigObj, ConfigObjError
		except ImportError:
			E(_("Python module ConfigObj is not installed. Please install it."))
			return 1

		configobj_options = {
			'file_error': True,
			'interpolation': False,
			'stringify': False
		}

		try:
			config = ConfigObj(config_path, configobj_options)
		except (IOError, ConfigObjError), e:
			E(_("Configuration file error: %s") % EE(e)) # Точку не надо - она уже есть в сообщении
			return 1
		# Читаем конфигурационный файл <--


		# Раскидываем прочитанные опции по своим местам -->

		general_options = (
			"backup_root",
			"backup_format",
			"backups_per_group",
			"groups_per_backup_root",
			"hash_size",
			"send_email_report",
			"mail_to",
			"mail_from",
			"mail_program"
		)

		entry_options = (
			"start_before",
			"start_after",
			"filter_default_policy",
			"filters"
		)

		for name, val in config.iteritems():

			if name in general_options and isinstance(val, str):

				if name == "backup_root":

					val = os.path.normpath(os.path.expanduser(val))

					if not os.path.isabs(val):
						E(_("%s '%s' value '%s' is not an absolute path.") % (config_error_prefix, name, val))
						return 1

					self.backup_root = val


				elif name == "backup_format":

					allowed_values = ("7z", "bz2", "gz", "tar")
					if val not in allowed_values:
						E(_("%s bad '%s' value '%s'. Allowed values: %s.") % (config_error_prefix, name, val, allowed_values))
						return 1

					self.backup.format = val


				elif name in ("backups_per_group", "groups_per_backup_root"):

					try:
						val = int(val)
					except ValueError:
						E(_("%s bad '%s' value '%s'.") % (config_error_prefix, name, val))
						return 1

					if val < 0:
						E(_("%s bad '%s' value '%s'.") % (config_error_prefix, name, val))
						return 1

					setattr(self.backup, name, val)


				elif name == "hash_size":

					try:
						val = int(val)
					except ValueError:
						E(_("%s bad '%s' value '%s'.") % (config_error_prefix, name, val))
						return 1

					allowed_values = (128, 224, 256, 384, 512)
					if val not in allowed_values:
						E(_("%s bad '%s' value '%s'. Allowed values: %s.") % (config_error_prefix, name, val, allowed_values))
						return 1

					self.backup.hash_size = val


				elif name == "send_email_report":

					if val.upper() in ("TRUE", "ON", "YES", "1"):
						self.backup.send_email_report = 1
					elif val.upper() in ("FALSE", "OFF", "NO", "0"):
						self.backup.send_email_report = 0
					else:
						E(_("%s bad '%s' value '%s'.") % (config_error_prefix, name, val))
						return 1


				elif name in ("mail_to", "mail_from"):

					if len(val.splitlines()) != 1:
						E(_("%s bad '%s' value: it can't be a multiline string.") % (config_error_prefix, name))
						return 1

					setattr(self.backup, name, val)


				elif name == "mail_program":
					self.backup.mail_program = val


				else:
					E(_("Logical error!"))
					return 1


			elif name in general_options and not isinstance(val, str):
				E(_("%s bad '%s' type.") % (config_error_prefix, name))
				return 1


			elif isinstance(val, dict):

				name = os.path.normpath(os.path.expanduser(name))

				if not os.path.isabs(name):
					E(_("%s backup entry path '%s' is not an absolute path.") % (config_error_prefix, name))
					return 1

				backup_entry = Backup_entry(name)

				for name, val in val.iteritems():

					if name in entry_options and isinstance(val, str):

						if name in ("start_before", "start_after"):
							setattr(backup_entry, name, val)


						elif name == "filter_default_policy":

							allowed_values = ("+", "-")
							if val not in allowed_values:
								E(_("%s bad '%s' value '%s' for backup entry '%s'. Allowed values: %s.") % (config_error_prefix, name, val, backup_entry.name, allowed_values))
								return 1

							backup_entry.filter_default_policy = val


						elif name == "filters":

							for line in val.splitlines():
								# Отрезаем символы табуляции пробелы в начале строки -->
								matches = STRIP_START_SPACES_RE.search(line)
								if matches:
									line = matches.group(1)
								# Отрезаем символы табуляции пробелы в начале строки <--

								matches = FILTER_LINE_RE.search(line)
								if matches:
									path_filter = Struct()
									path_filter.policy = matches.group(1)
									path_filter.string = line

									# Комментарий
									if path_filter.policy == "#":
										continue

									try:
										path_filter.re = re.compile(matches.group(2))
									except re.error:
										E(_("%s bad filter regex string for backup entry '%s': '%s'.") % (config_error_prefix, backup_entry.name, matches.group(2)))
										return 1

									backup_entry.filters.append(path_filter)
								elif BLANK_LINE_RE.search(line):
									continue
								else:
									E(_("%s bad filter string for backup entry '%s': '%s'.") % (config_error_prefix, backup_entry.name, line))
									return 1


						else:
							E(_("Logical error!"))
							return 1


					elif name in entry_options and not isinstance(val, str):
						E(_("%s bad '%s' type for backup entry '%s'.") % (config_error_prefix, name, backup_entry.name))
						return 1


					else:
						E(_("%s unknown option '%s' in backup entry '%s'.") % (config_error_prefix, name, backup_entry.name))
						return 1

				self.backup.entries.append(backup_entry)


			else:
				E(_("%s unknown option '%s'.") % (config_error_prefix, name))
				return 1

		D(_("Backup config dump:\n%s") % repr(self.backup))
		# Раскидываем прочитанные опции по своим местам <--

		# Проверяем обязательные опции -->
		if not self.backup_root:
			E(_("%s 'backup_root' option not found in config. It must exists.") % config_error_prefix)
			return 1

		if not len(self.backup.entries):
			E(_("%s no backup entries found in config. At least one must exists.") % config_error_prefix)
			return 1
		# Проверяем обязательные опции <--

		# Проверяем настройки email отчетов -->
		if self.backup.send_email_report:
			if not self.backup.mail_to:
				E(_("%s option 'send_email_report' set to true, but 'mail_to' option is not set.") % config_error_prefix)
				return 1
			if not self.backup.mail_program:
				E(_("%s option 'send_email_report' set to true, but 'mail_program' option is not set.") % config_error_prefix)
				return 1
		# Проверяем настройки email отчетов <--

		return 0



	def restore_mode(self, path):

		self.restore = Struct()
		try:
			self.restore.src_path = os.path.abspath(os.path.normpath(path))
		except OSError, e:
			E(_("Can't get '%s' absolute path: %s.") % (path, EE(e)))
			return 1
		self.restore.dest_path = os.path.join(".", os.path.basename(self.restore.src_path))

		# Определяем backup root.
		# На FS уровне будет его проверка.
		self.backup_root = os.path.dirname(os.path.dirname(self.restore.src_path))

		# Получаем имя бэкапа и группу
		self.restore.src_backup = os.path.basename(self.restore.src_path)
		self.restore.src_group = os.path.basename(os.path.dirname(self.restore.src_path))

		return 0

