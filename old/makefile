app_name := pyvsb
prefix := /usr
bin_dir := $(prefix)/bin
lib_dir := $(prefix)/lib/$(app_name)
doc_dir := $(prefix)/share/doc/$(app_name)
locale_dir := $(prefix)/share/locale
po_dir = po
deb_package_dir := deb

.PHONY: all install uninstall deb deb_clean clean


all: $(patsubst $(po_dir)/%.po,$(po_dir)/%.mo,$(wildcard $(po_dir)/*.po)) $(patsubst %.py,%.pyc,$(wildcard *.py))


$(po_dir)/%.mo: $(po_dir)/%.po
	output=$$(xgettext --force-po *.py -o '$(patsubst %.po,%.pot,$<)' 2>&1) || { echo "$$output" >&2; exit 1; }
	msgcomm --no-location '$<' '$(patsubst %.po,%.pot,$<)' -o '$<'
	rm -f '$(patsubst %.po,%.pot,$<)'
	output=$$(xgettext --sort-by-file --join-existing --force-po --omit-header *.py -o '$<' 2>&1) || { echo "$$output" >&2; exit 1; }
	if [ '$<' = '$(po_dir)/en.po' ]; then msgen --force-po '$<' -o '$<'; fi
	msgfmt '$<' -o '$@'


%.pyc: %.py
	python -c 'import py_compile, sys; py_compile.compile("$<")'


install: all uninstall $(patsubst $(po_dir)/%.mo,$(locale_dir)/%/LC_MESSAGES/$(app_name).mo,$(wildcard $(po_dir)/*.mo))
	mkdir -m 755 -p '$(lib_dir)'
	for file in *.py *.pyc; \
	do \
		if [ "$$file" = "main.py" ]; \
		then \
			install -m 755 "$$file" "$(lib_dir)/$$file" || exit 1; \
		elif [ "$$file" = "main.pyc" ]; \
		then \
			:; \
		else \
			install -m 644 "$$file" "$(lib_dir)/$$file" || exit 1; \
		fi \
	done
	\
	mkdir -m 755 -p '$(bin_dir)'
	ln -s '../lib/$(app_name)/main.py' '$(bin_dir)/$(app_name)'
	\
	mkdir -m 755 -p '$(doc_dir)'
	cp -r doc/* $(doc_dir)


$(locale_dir)/%/LC_MESSAGES/$(app_name).mo: $(po_dir)/%.mo
	mkdir -m 755 -p "$$(dirname '$@')"
	install -m 644 '$<' '$@'


uninstall:
	rm -f $(patsubst $(po_dir)/%.po,$(locale_dir)/%/LC_MESSAGES/$(app_name).mo,$(wildcard $(po_dir)/*.po))
	rm -f '$(bin_dir)/$(app_name)'
	rm -rf '$(lib_dir)'
	rm -rf '$(doc_dir)'


deb: deb_clean
	if [ "$$(whoami)" != 'root' ]; \
	then \
		echo "You must be root to do this." >&2; exit 1; \
	fi
	\
	$(MAKE)
	\
	mkdir -p '$(deb_package_dir)/usr'
	$(MAKE) install prefix='$(deb_package_dir)/usr'
	\
	mkdir -p '$(deb_package_dir)/DEBIAN'
	cd '$(deb_package_dir)' && md5sum $$(find usr -type f) | sed -r 's/^([a-zA-Z0-9]+ +)/\1\//g' > DEBIAN/md5sums
	echo "Installed-Size: $$(du -s '$(deb_package_dir)/usr' | awk '{ print $$1 }')" > '$(deb_package_dir)/DEBIAN/control'
	cat deb_control >> '$(deb_package_dir)/DEBIAN/control'
	\
	dpkg-deb -b '$(deb_package_dir)' $(app_name)_$$(egrep '^Version:[[:space:]]+[0-9.]+$$' '$(deb_package_dir)/DEBIAN/control' | sed -r 's/^Version:[[:space:]]+([0-9.]+)$$/\1/')_all.deb


deb_clean:
	rm -rf '$(deb_package_dir)';
	rm -f *.deb


clean: deb_clean
	rm -f '$(po_dir)'/*.mo '$(po_dir)'/*.pot
	rm -f *.pyc

