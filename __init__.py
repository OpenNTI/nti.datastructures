#!/usr/bin/env python

# Note that we're not exporting anything by importing it.
# This helps reduce the chances of import cycles

# XXX Import side-effects.
# Loading this file monkey-patches sockets and ssl to work with gevent.
# This is needed for the openid handling in logon.py, but doing it here is a bit
# earlier and has a greater chance of working. This is also after
# we have loaded ZODB and doesn't seem to interfere with it. See gunicorn.py.
# NOTE: 1.0 of gevent seems to fix the threading issue that cause problems with ZODB.
# Try to confirm that
import logging
logger = logging.getLogger(__name__)


import gevent
import gevent.monkey
if getattr( gevent, 'version_info', (0,) )[0] >= 1:
	logger.info( "Monkey patching most libraries for gevent" )
	# omit thread, it's required for multiprocessing futures, used in contentrendering
	# This is true even of the builds as-of 20120508 that have added a 'subprocess' module;
	# it would be nice to fix (so we get greenlet names in the logs instead of always "MainThread",
	# plus would eliminate the need to manually patch these things).
	gevent.monkey.patch_all(thread=False,subprocess=False)
	# The problem is that multiprocessing.queues.Queue uses a half-duplex multiprocessing.Pipe,
	# which is implemented with os.pipe() and _multiprocessing.Connection. os.pipe isn't patched
	# by gevent, as it returns just a fileno. _multiprocessing.Connection is an internal implementation
	# class implemented in C, which exposes a 'poll(timeout)' method; under the covers, this issues a
	# (blocking) select() call: hence the need for a real thread. Except for that method, we could
	# almost replace Connection with gevent.fileobject.SocketAdapter, plus a trivial
	# patch to os.pipe (below). Sigh, so close. (With a little work, we could replicate that method)

	# import os
	# import fcntl
	# os_pipe = os.pipe
	# def _pipe():
	#	r, w = os_pipe()
	#	fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
	#	fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
	#	return r, w
	#os.pipe = _pipe

	# Even if we don't patch threads, thread locals we MUST patch
	import gevent.local
	import threading
	threading.local = gevent.local.local
	_threading_local = __import__('_threading_local')
	_threading_local.local = gevent.local.local

	# depending on the order of imports, we may need to patch
	# some things up manually. TODO: This list is not complete.
	import transaction
	if gevent.local.local not in transaction.ThreadTransactionManager.__bases__:
		class GeventTransactionManager(transaction.TransactionManager):
			pass
		manager = GeventTransactionManager()
		transaction.manager = manager
		transaction.get = transaction.__enter__ = manager.get
		transaction.begin = manager.begin
		transaction.commit = manager.commit
		transaction.abort = manager.abort
		transaction.__exit__ = manager.__exit__
		transaction.doom = manager.doom
		transaction.isDoomed = manager.isDoomed
		transaction.savepoint = manager.savepoint
		transaction.attempts = manager.attempts

	import zope.component
	import zope.component.hooks
	if gevent.local.local not in type(zope.component.hooks.siteinfo).__bases__:
		# TODO: Is there a better way to do this?
		# This code is copied from zope.component 3.12
		class SiteInfo(threading.local):
			site = None
			sm = zope.component.getGlobalSiteManager()

			def adapter_hook(self):
				adapter_hook = self.sm.adapters.adapter_hook
				self.adapter_hook = adapter_hook
				return adapter_hook

			adapter_hook = zope.component.hooks.read_property(adapter_hook)

		zope.component.hooks.siteinfo = SiteInfo()
		del SiteInfo

	del zope
	del transaction
	del threading
	del _threading_local
else:
	logger.info( "Monkey patching minimum libraries for gevent" )
	gevent.monkey.patch_socket(); gevent.monkey.patch_ssl()

del gevent


# Patch zope.component.hooks.site to not be broken
from zope.component.hooks import setSite, getSite


def _patch_site():
	import zope.component.hooks
	from zope.interface.registry import Components

	class Site(object):
		def __init__(self):
			self.registry = Components('components')
		def getSiteManager(self):
			return self.registry


	def is_broken():
		site = Site()
		old_site = getSite()
		try:
			with zope.component.hooks.site(site):
				raise ValueError()
			assert None, "Should not get here"
		except ValueError:
			broken = getSite() is not old_site
			if broken:
				# Fixup!
				setSite( old_site )
			return broken
		else:
			assert None, "Should not get here"


	# We require 3.12.1+ which fixes this problem

	assert not is_broken(), "Brokenness should be fixed in 3.12.1+"

_patch_site()

del _patch_site
del logger
del logging
del setSite
del getSite
