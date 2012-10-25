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

PROGRAM_COMMAND_NAME = "pyvsb"
PROGRAM_VERSION = "0.2"

# Настраиваем работу gettext -->
import gettext

try:
    gettext.textdomain(PROGRAM_COMMAND_NAME)
    gettext.install(PROGRAM_COMMAND_NAME)
except IOError, e:
	__builtins__.__dict__["_"] = lambda x : x
	E("Gettext error: %s." % EE(e))
# Настраиваем работу gettext <--

# Обработчик прерывания с клавиатуры -->
import signal
import sys

IS_PROGRAM_CLOSING = 0

def signal_interruption_handler(signal, frame):
	global IS_PROGRAM_CLOSING

	if not IS_PROGRAM_CLOSING:
		IS_PROGRAM_CLOSING = 1
		E(_("Program has been interrupted by SIGINT."))
		sys.exit(0)

signal.signal(signal.SIGINT, signal_interruption_handler)
# Обработчик прерывания с клавиатуры <--

import os
import time
from getopt import gnu_getopt, GetoptError
from string import Template

from lib import *

DEFAULT_BACKUP_CONFIG = "~/." + PROGRAM_COMMAND_NAME + ".conf"
HELP_TEXT = _("""\
${program_name} ${program_version}  Copyright (c) ${copyright_years} Konishchev Dmitry

Usage: ${program_command_name} [OPTIONS] [PATH]

Options:
  -r, --restore    - restore mode
  -d, --debug      - show debug messages
  -h, --help       - show this help
  --version        - show program version

Examples:
  # Create backup with default configuration file (${default_config})
  ${program_command_name}

  # Create backup with configuration file example.cfg
  ${program_command_name} example.cfg

  # Restore backup '~/backups/current/2008.08.04_23.31.47'
  # to ./2008.08.04_23.31.47
  ${program_command_name} -r ~/backups/current/2008.08.04_23.31.47

Files:
  ${default_config} - default configuration file.""")



def main():

	mode = "b"

	# Парсим опции командной строки -->
	try:
		options, args = gnu_getopt(sys.argv[1:], 'dhr', ["debug", "help", "restore", "version"])
		
		for option, value in options:
			if option in ("-d", "--debug"):
				LOG.set_debug_level()
			elif option in ("-h", "--help"):
				show_help()
				return 0
			elif option in ("-r", "--restore"):
				mode = "r"
			elif option in ("--version"):
				show_version()
				return 0
			else:
				E(_("Logical error!"))
				raise Function_error

		if len(args) > 1:
			raise Function_error
		elif len(args) == 1:
			path = args[0]
		elif len(args) == 0 and mode == "b":
			path = os.path.expanduser(DEFAULT_BACKUP_CONFIG)
		else:
			raise Function_error
	except (GetoptError, Function_error):
		show_usage()
		return 1
	# Парсим опции командной строки <--

	if mode == "b":
		D(_("Starting backup mode..."))

		from backup import Backup, Backup_error

		try:
			Backup(path)
		except Backup_error:
			E(_("Backup with configuration file '%s' failed.") % path)
			return 1
	else:
		D(_("Starting restore mode..."))

		from restore import Restore, Restore_error

		try:
			Restore(path)
		except Restore_error:
			E(_("Restoring of '%s' failed.") % path)
			return 1



def show_help():

	start_year = "2008"
	end_year = time.strftime("%Y", time.localtime())

	if start_year == end_year:
		copyright_years = start_year
	else:
		copyright_years = start_year + "-" + end_year

	print Template(HELP_TEXT).substitute({
		'program_name': PROGRAM_NAME,
		'program_command_name': PROGRAM_COMMAND_NAME,
		'program_version': PROGRAM_VERSION,
		'copyright_years': copyright_years,
		'default_config': DEFAULT_BACKUP_CONFIG
	})



def show_usage():
	print _("Usage: %s -h") % PROGRAM_COMMAND_NAME



def show_version():
	print _("%s %s") % (PROGRAM_NAME, PROGRAM_VERSION)



if __name__ == "__main__":
	sys.exit(main())

