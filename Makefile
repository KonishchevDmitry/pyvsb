.PHONY: build check install dist pypi clean

PROJECT := pyvsb
PYTHON := python3

build:
	$(PYTHON) setup.py build

check:
	$(PYTHON) setup.py test

install:
	$(PYTHON) setup.py install --skip-build

dist: clean
	$(PYTHON) setup.py sdist

pypi: clean
	$(PYTHON) setup.py sdist upload

clean:
	rm -rf build dist $(PROJECT).egg-info *.egg
