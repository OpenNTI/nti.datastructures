""" Tests for the dataserver. """

from unittest import defaultTestLoader
from unittest import TextTestRunner
import os

# for export
from nti.tests import has_attr, implements, Implements, provides, Provides

def runner(path, pattern="*.py"):
	suite = defaultTestLoader.discover(path, pattern)
	try:
		runner = TextTestRunner(verbosity=3)
		for test in suite:
			runner.run(test)
	finally:
		pass

def main():
	dirname = os.path.dirname( __file__ )
	if not dirname:
		dirname = '.'
	runner( dirname )

if __name__ == '__main__':
	main()

