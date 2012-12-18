# Copyright (C) 2008 Richard Hughes <richard@hughsie.com>
#
# Licensed under the GNU General Public License Version 2
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

command_not_found_handle () {
	runcnf=1
	retval=127

	# don't run if DBus isn't running
	[ ! -S /var/run/dbus/system_bus_socket ] && runcnf=0

	# don't run if packagekitd doesn't exist in the _system_ root
	[ ! -x /usr/libexec/packagekitd ] && runcnf=0

	# run the command, or just print a warning
	if [ $runcnf -eq 1 ]; then
		/usr/libexec/pk-command-not-found $@
		retval=$?
	else
		echo "bash: $1: command not found"
	fi

	# return success or failure
	return $retval
}

