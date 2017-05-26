"""PyVSB installation script."""

from __future__ import unicode_literals

from setuptools import find_packages, setup
from setuptools.command.test import test as Test


class PyTest(Test):
    def finalize_options(self):
        Test.finalize_options(self)
        self.test_args = [
            "tests/test.py",
            #"--capture=no",
        ]
        self.test_suite = True

    def run_tests(self):
        import pytest
        pytest.main(self.test_args)


if __name__ == "__main__":
    with open("README") as readme:
        setup(
            name = "pyvsb",
            version = "0.4",

            description = readme.readline().strip(),
            long_description = readme.read().strip(),
            url = "https://github.com/KonishchevDmitry/pyvsb",

            license = "GPL3",
            author = "Dmitry Konishchev",
            author_email = "konishchev@gmail.com",

            classifiers = [
                "Environment :: Console",
                "Intended Audience :: End Users/Desktop",
                "Intended Audience :: System Administrators",
                "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
                "Natural Language :: English",
                "Operating System :: MacOS :: MacOS X",
                "Operating System :: POSIX",
                "Operating System :: Unix",
                "Programming Language :: Python :: 3",
                "Topic :: System :: Archiving :: Backup",
                "Topic :: Utilities",
            ],
            platforms = [ "unix", "linux", "osx" ],

            install_requires = [ "psys", "psh" ],
            packages = find_packages(),

            cmdclass = { "test": PyTest },
            tests_require = [ "psys", "pytest" ],

            entry_points = {
                "console_scripts": [ "pyvsb = pyvsb.main:main" ],
            },
        )
