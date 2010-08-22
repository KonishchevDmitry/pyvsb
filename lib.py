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
import StringIO
import gzip
import logging
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time



BUF_SIZE = 1024 * 16

DIRS_MODE = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
FILES_MODE = stat.S_IRUSR | stat.S_IWUSR

BLANK_LINE_RE = re.compile("""^\s*$""")

MAX_RECURSION_DEPTH = sys.getrecursionlimit() / 2

PROGRAM_NAME = "PyVSB"



# Исключение, которое функции генерируют при ошибке
class Error(Exception):
	pass



# Исключение, которое функции используют внутри себя
class Function_error(Exception):
	pass



class Path_error(Exception):
	pass



class Struct:

	def __repr__(self):

		struct_dict = {}

		for name in dir(self):
			if name[0:1] != "_":
				struct_dict[name] = getattr(self, name)

		return repr(struct_dict)



# Инструментарий для работы с логом -->
class Log:

	current_log_level = logging.INFO

	def __init__(self):

		# Назначаем имена уровням лога -->
		logging.addLevelName(logging.DEBUG, "D")
		logging.addLevelName(logging.INFO, "I")
		logging.addLevelName(logging.WARNING, "W")
		logging.addLevelName(logging.ERROR, "E")
		# Назначаем имена уровням лога <--

		# Debug -->
		self.debug_handler = logging.StreamHandler(sys.stdout)
		self.debug_handler.setLevel(logging.DEBUG)
		self.set_format(self.debug_handler)

		self.debug_logger = logging.getLogger("debug")
		self.debug_logger.addHandler(self.debug_handler)
		# По умолчанию ставим уровень выше debug
		self.debug_logger.setLevel(self.get_current_log_level())
		self.debug_logger.propagate = False
		# Debug <--

		# Info -->
		self.info_handler = logging.StreamHandler(sys.stdout)
		self.info_handler.setLevel(logging.INFO)
		self.set_format(self.info_handler)

		self.info_logger = logging.getLogger("info")
		self.info_logger.addHandler(self.info_handler)
		self.info_logger.setLevel(logging.INFO)
		self.info_logger.propagate = False
		# Info <--

		# Warning -->
		self.warning_handler = logging.StreamHandler(sys.stderr)
		self.warning_handler.setLevel(logging.WARNING)
		self.set_format(self.warning_handler)

		self.warning_logger = logging.getLogger("warning")
		self.warning_logger.addHandler(self.warning_handler)
		self.warning_logger.setLevel(logging.WARNING)
		self.warning_logger.propagate = False
		# Warning <--

		# Error -->
		self.error_handler = logging.StreamHandler(sys.stderr)
		self.error_handler.setLevel(logging.ERROR)
		self.set_format(self.error_handler)

		self.error_logger = logging.getLogger("error")
		self.error_logger.addHandler(self.error_handler)
		self.error_logger.setLevel(logging.ERROR)
		self.error_logger.propagate = False
		# Error <--



	def add_handler(self, handler):
		self.debug_logger.addHandler(handler)
		self.info_logger.addHandler(handler)
		self.warning_logger.addHandler(handler)
		self.error_logger.addHandler(handler)



	def get_current_log_level(self):
		return self.current_log_level



	def remove_handler(self, handler):
		self.debug_logger.removeHandler(handler)
		self.info_logger.removeHandler(handler)
		self.warning_logger.removeHandler(handler)
		self.error_logger.removeHandler(handler)



	def set_format(self, handler):
		if self.get_current_log_level() == logging.DEBUG:
			formatter = logging.Formatter("(%(asctime)s.%(msecs)03d) (%(filename)12.12s:%(lineno)03d): %(levelname)s: %(message)s", "%H:%M:%S")
		else:
			formatter = logging.Formatter("%(levelname)s: %(message)s")

		handler.setFormatter(formatter)



	def set_debug_level(self):
		self.current_log_level = logging.DEBUG

		self.debug_logger.setLevel(self.get_current_log_level())

		self.set_format(self.debug_handler)
		self.set_format(self.info_handler)
		self.set_format(self.warning_handler)
		self.set_format(self.error_handler)



class String_log:

	def __init__(self):

		self.stream = StringIO.StringIO()

		self.handler = logging.StreamHandler(self.stream)
		self.handler.setLevel(LOG.get_current_log_level())
		LOG.set_format(self.handler)

		LOG.add_handler(self.handler)



	def __del__(self):
		LOG.remove_handler(self.handler)



	def get(self):

		self.handler.flush()

		try:
			return self.stream.getvalue()
		except UnicodeError, e:
			return _("E: Formatting log for output failed with Unicode conversion error: %s.") % EE(e)



class Log_errors_counter:

	def __init__(self):

		self.handler = Log_errors_counter_handler()
		self.handler.setLevel(logging.WARNING)

		LOG.add_handler(self.handler)



	def __del__(self):
		LOG.remove_handler(self.handler)



	def get(self):
		return self.handler.get()



class Log_errors_counter_handler(logging.Handler):

	def __init__(self):

		logging.Handler.__init__(self)

		self.warnings = 0
		self.errors = 0



	def emit(self, record):

		if record.levelno == logging.WARNING:
			self.warnings += 1
		elif record.levelno == logging.ERROR:
			self.errors += 1



	def get(self):
		return (self.warnings, self.errors)



LOG = Log()

# Функции, с помощью которых сообщения будут
# записываться в лог.
# -->
D = LOG.debug_logger.debug
I = LOG.info_logger.info
W = LOG.warning_logger.warning
E = LOG.error_logger.error
# <--
# Инструментарий для работы с логом <--



def chmod(path, mode):
	try:
		os.chmod(path, mode)
	except OSError, e:
		E(_("Can't chmod '%s': %s.") % (path, EE(e)))
		return 1

	return 0



def decompress_file(src_path, dest_path):

	file_name, file_ext = os.path.splitext(src_path)

	file_name = os.path.basename(file_name)
	file_ext = file_ext[1:]

	if file_ext not in ("bz2", "gz"):
		E(_("Can't decompress file '%s' - it is not *.bz2 or *.gz file.") % src_path)
		return 1

	try:

		try:
			if file_ext == "bz2":
				src_file = bz2.BZ2File(src_path, "r")
			else:
				src_file = gzip.GzipFile(src_path, "r")
		except IOError, e:
			E(_("Error while reading compressed file '%s': %s.") % (src_path, EE(e)))
			raise Function_error

		try:
			dest_file_fd = None
			dest_file_fd = os.open(dest_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_LARGEFILE, FILES_MODE)
			dest_file = os.fdopen(dest_file_fd, "w")
		except (OSError, ValueError, IOError), e:
			E(_("Can't create file '%s': %s.") % (dest_path, EE(e)))

			if dest_file_fd != None:
				try:
					os.close(dest_file_fd)
				except OSError:
					pass

			raise Function_error

		try:
			shutil.copyfileobj(src_file, dest_file)
		except (OSError, IOError), e:
			E(_("Error while copying file '%s' to '%s'. '%s': %s.") % (src_path, dest_path, e.filename, EE(e)))
			raise Function_error

		try:
			src_file.close()
			dest_file.close()
		except IOError, e:
			E(_("Can't close file '%s': %s.") % (e.filename, EE(e)))
			raise Function_error

	except Function_error:
		E(_("Decompressing of file '%s' failed.") % src_path)
		return 1

	return 0



def EE(e):
	if hasattr(e, "strerror") and e.strerror:
		return e.strerror
	elif hasattr(e, "message") and e.message:
		return e.message
	else:
		return "%s" % e



def get_link_abs_path(file_info):
	if os.path.isabs(file_info.linkname):
		return file_info.linkname
	else:
		return os.path.normpath(os.path.join(os.path.dirname("/" + file_info.name), file_info.linkname))



def is_dir(path):
	try:
		file_lstat = os.lstat(path)

		if stat.S_ISDIR(file_lstat[stat.ST_MODE]):
			return 1
		else:
			return 0
	except OSError, e:
		E(_("Can't stat '%s': %s.") % (path, EE(e)))
		raise Path_error



def is_exists(path):

	if path == "/":
		return 1

	dir_path = os.path.dirname(path)

	try:
		dir_list = os.listdir(dir_path)
	except OSError, e:
		E(_("Can't read directory '%s': %s.") % (dir_path, EE(e)))
		raise Path_error

	if os.path.basename(path) in dir_list:
		return 1
	else:
		return 0



def mkdir(path):
	try:
		os.mkdir(path, DIRS_MODE)
	except OSError, e:
		E(_("Can't create directory '%s': %s.") % (path, EE(e)))
		return 1

	return 0



def mkdtemp(dest_dir = ""):
	try:
		if dest_dir:
			return tempfile.mkdtemp(dir = dest_dir)
		else:
			return tempfile.mkdtemp()
	except OSError, e:
		if dest_dir:
			E(_("Can't create temp directory in '%s': %s.") % (dest_dir, EE(e)))
		else:
			E(_("Can't create temp directory: %s.") % EE(e))
		return 1

	return 0



def mkstemp(dest_dir = ""):
	try:
		if dest_dir:
			return tempfile.mkstemp(dir = dest_dir)[1]
		else:
			return tempfile.mkstemp()[1]
	except (OSError, IOError), e:
		if dest_dir:
			E(_("Can't create temp file in '%s': %s.") % (dest_dir, EE(e)))
		else:
			E(_("Can't create temp file: %s.") % EE(e))
		return ""



def move(src_path, dest_path):
	try:
		shutil.move(src_path, dest_path)
	except (OSError, IOError), e:
		E(_("Can't move '%s' to '%s': %s.") % (src_path, dest_path, EE(e)))
		return 1

	return 0



def rename(src_path, dest_path):
	try:
		os.rename(src_path, dest_path)
	except OSError, e:
		E(_("Can't rename '%s' to '%s': %s.") % (src_path, dest_path, EE(e)))
		return 1

	return 0



def rm(path):
	"""Удаляет файл или дерево каталогов."""

	try:
		if is_dir(path):
			try:
				shutil.rmtree(path)
			except (OSError, IOError), e:
				E(_("Can't delete '%s': %s.") % (path, EE(e)))
				return 1
		else:
			return unlink(path)
	except Path_error:
		E(_("Deleting of '%s' failed.") % path)
		return 1

	return 0



def rm_if_exists(path):
	try:
		if is_exists(path):
			return rm(path)
	except Path_error:
		E(_("Deleting of '%s' failed.") % path)
		return 1

	return 0



def run_command(command, input_string = ""):
	"""Возвращает (exit_status, shell_output) или генерирует Error в случае ошибки."""

	D(_("Running command '%s'...") % command)

	try:

		# Создаем pipe, в который будут направляться stdout и stderr -->
		pipe = []

		try:
			pipe = list(os.pipe())

			read_pipe = os.fdopen(pipe[0], "r")
			pipe[0] = None

			write_pipe = os.fdopen(pipe[1], "w")
			pipe[1] = None
		except (OSError, IOError, ValueError), e:
			E(_("Can't create a pipe: %s.") % EE(e))

			for fd in pipe:
				if fd != None:
					try:
						os.close(fd)
					except OSError:
						pass

			raise Function_error
		# Создаем pipe, в который будут направляться stdout и stderr <--

		# Запускаем процесс -->
		try:
			process = subprocess.Popen(command, stdin = subprocess.PIPE, stdout = write_pipe, stderr = write_pipe, shell = True)
		except (OSError, ValueError), e:
			E(_("Running command '%s' failed: %s.") % (command, EE(e)))
			raise Error
		# Запускаем процесс <--

		# Передаем процессу в stdin все необходимые данные -->
		try:
			if input_string:
				process.stdin.write(input_string)
		except IOError, e:
			# Ничего особо страшного в этом нет - просто, скорее всего,
			# процесс завершился раньше, чем мы начали писать ему в stdin.
			D(_("Can't write data to process stdin: %s.") % EE(e))

		try:
			# Чтобы процесс не ждал ввода от пользователя
			process.stdin.close()
		except IOError, e:
			E(_("Can't close command '%s' stdin pipe descriptor: %s.") % (command, EE(e)))
			I(_("Killing started command process..."))

			# Убиваем только что созданный процесс -->
			try:
				os.kill(process.pid, signal.SIGINT)
				time.sleep(3)
				os.kill(process.pid, signal.SIGKILL)
			except OSError:
				pass
			# Убиваем только что созданный процесс <--

			process.wait()

			raise Function_error
		# Передаем процессу в stdin все необходимые данные <--

		# Ждем завершения процесса
		exit_status = process.wait()

		# Получаем stdout и stdin процесса -->
		try:
			write_pipe.close()
			shell_output = read_pipe.read()
			read_pipe.close()
		except IOError, e:
			shell_output = _("Error! Can't get command '%s' output: %s.") % (command, EE(e))
		# Получаем stdout и stdin процесса <--

		D(_("Command '%s' exit status: %d.") % (command, exit_status))

	except Function_error:
		E(_("Running command '%s' failed.") % command)
		raise Error

	return (exit_status, shell_output)



def send_email_message(email_program, from_address, to_address, subject, message):

	import email.mime.text
	import email.errors
	import locale

	D(_("Sending email message from '%s' to '%s' by email program '%s'...") % (from_address, to_address, email_program))

	try:
		message = email.mime.text.MIMEText(message)
		if from_address:
			message['From'] = from_address
		message['To'] = to_address
		message['Subject'] = subject
		message.set_charset(locale.getdefaultlocale()[1])
		message = message.as_string()
	except email.errors.MessageError, e:
		E(_("Can't create email message: %s") % EE(e))
		return 1

	try:
		exit_status, shell_output = run_command(email_program, message)
		if exit_status:
			E(_("Sending email message failed. Email program return exit status %d. Shell output:\n%s") % (exit_status, shell_output))
			return 1
	except Error:
		E(_("Sending email message failed."))
		return 1

	D(_("Email message has been sent."))

	return 0



def unlink(path):
	try:
		os.unlink(path)
	except OSError, e:
		E(_("Can't delete file '%s': %s.") % (path, EE(e)))
		return 1

	return 0



def unlink_if_exists(path):
	try:
		if is_exists(path):
			return unlink(path)
	except Path_error:
		E(_("Removing '%s' failed."))
		return 1

	return 0

