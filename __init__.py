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

import sys

import gevent
import gevent.monkey
PATCH_THREAD = True

if getattr( gevent, 'version_info', (0,) )[0] >= 1 and 'ZEO' not in sys.modules: # Don't do this when we are loaded for conflict resolution into somebody else's space
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

	# However, there is a more serious conflict. We MUST have greenlet local things like
	# transactions. We can do that with some careful patching. But then we must have
	# greenlet-aware locks. If we patch them as well, then the ProcessPoolExecutor fails.
	# Basically there's a conflict between multiprocessing and greenlet locks, or Real threads
	# and greenlet locks.
	# So it turns out to be easier to patch the ProcessPoolExecutor to use "threads"
	# and patch the threading system.
	import gevent.local
	import threading
	import thread
	from gevent import thread as green_thread
	_threading_local = __import__('_threading_local')
	if PATCH_THREAD:
		gevent.monkey.patch_thread()
		gevent.monkey.patch_subprocess()
		import concurrent.futures
		import multiprocessing
		concurrent.futures._ProcessPoolExecutor = concurrent.futures.ProcessPoolExecutor
		def ProcessPoolExecutor( max_workers=None ):
			if max_workers is None:
				max_workers = multiprocessing.cpu_count()
			return concurrent.futures.ThreadPoolExecutor( max_workers )
		concurrent.futures.ProcessPoolExecutor = ProcessPoolExecutor

		# Patch for try/finally missing in ZODB 3.10.5
		def tpc_begin(self, txn, tid=None, status=' '):
			"""Storage API: begin a transaction."""
			if self._is_read_only:
				raise POSException.ReadOnlyError()
			logger.debug( "Taking tpc lock %s %s for %s", self, self._tpc_cond, txn )
			self._tpc_cond.acquire()
			try:
				self._midtxn_disconnect = 0
				while self._transaction is not None:
					# It is allowable for a client to call two tpc_begins in a
					# row with the same transaction, and the second of these
					# must be ignored.
					if self._transaction == txn:
						raise POSException.StorageTransactionError(
							"Duplicate tpc_begin calls for same transaction")

					self._tpc_cond.wait(30)
			finally:
				logger.debug( "Releasing tpc lock %s %s for %s", self, self._tpc_cond, txn )
				self._tpc_cond.release()

			self._transaction = txn

			try:
				self._server.tpc_begin(id(txn), txn.user, txn.description,
									   txn._extension, tid, status)
			except:
				# Client may have disconnected during the tpc_begin().
				if self._server is not disconnected_stub:
					self.end_transaction()
				raise

			self._tbuf.clear()
			self._seriald.clear()
			del self._serials[:]
		import ZEO.ClientStorage
		ZEO.ClientStorage.ClientStorage.tpc_begin = tpc_begin


		# The dummy-thread deletes __block, which interacts
		# badly with forking process with subprocess: after forking,
		# Thread.__stop is called, which throws an exception
		orig_stop = threading.Thread._Thread__stop
		def __stop(self):
			if hasattr( self, '_Thread__block' ):
				orig_stop( self )
			else:
				setattr( self, '_Thread__stopped', True )
		threading.Thread._Thread__stop = __stop

	# However, doing so reveals some sort of deadlock on ZODB committing that
	# the chat integration tests can trigger.
	else:
		threading.local = gevent.local.local

		_threading_local.local = gevent.local.local


	# depending on the order of imports, we may need to patch
	# some things up manually.
	# TODO: This list is not complete.
	# TODO: Since these things are so critical, we might should just throw
	# an exception and refuse to run of the import order is bad?
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

	from logging import LogRecord
	from gevent import getcurrent, Greenlet
	class _LogRecord(LogRecord):
		def __init__( self, *args, **kwargs ):
			LogRecord.__init__( self, *args, **kwargs )
			# TODO: Respect logging.logThreads?
			if self.threadName == 'MainThread':
				current = getcurrent()
				thread_info = getattr( current, '__thread_name__', None )
				if thread_info:
					self.thread = id(current)
					self.threadName = thread_info()

				elif type(current) == Greenlet \
				  or isinstance( current, Greenlet ):
					self.thread = id( current )
					self.threadName = current._formatinfo()

	logging.LogRecord = _LogRecord

	del _LogRecord
	del zope
	del transaction
	del threading
	del _threading_local
elif getattr( gevent, 'version_info', (0,) )[0] != 0:
	logger.info( "Monkey patching minimum libraries for gevent" )
	gevent.monkey.patch_socket(); gevent.monkey.patch_ssl()

del gevent


# Patch zope.component.hooks.site to not be broken, if necessary
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
del setSite
del getSite

# Patch zope.traversing.api.traverseName and zope.traversing.adapters.DefaultTraverser
# to be robust against unicode strings in attr names. Do this
# in-place to be sure that even if it's already imported (which is likely) the patches
# hold
from zope import interface as _zinterface
from zope.traversing import api as _zapi, adapters as _zadapters, interfaces as _zinterfaces
import sys

# Save the original implementation
_marker = _zadapters._marker
def _nti_traversePathElement( obj, name, further_path, default=_marker,
							  traversable=None, request=None): pass
_nti_traversePathElement.__code__ = _zadapters.traversePathElement.__code__
# Carefully add the right globals. Too much screws things up
_nti_traversePathElement.func_globals['ITraversable'] = _zinterfaces.ITraversable
_nti_traversePathElement.func_globals['LocationError'] = _zinterfaces.TraversalError
_nti_traversePathElement.func_globals['nsParse'] = _zadapters.nsParse
_nti_traversePathElement.func_globals['namespaceLookup'] = _zadapters.namespaceLookup

def _patched_traversePathElement(obj, name, further_path, default=_marker,
								 traversable=None, request=None ):
	try:
		return _nti_traversePathElement(obj, name, further_path, default=default,
										traversable=traversable, request=request)
	except UnicodeEncodeError:
		# Either raise as location error, or return the default
		# The default could have come in either as keyword or positional
		# argument.
		if default is not _marker:
			return default
		# These two things we get from the func_globals dictionary
		info = _nti_exc_info()
		raise LocationError( "Unable to traverse due to attempt to access attribute as unicode.",
							  obj, name ), None, info[2]
_patched_traversing = False
def _patch_traversing():

	@_zinterface.implementer(_zinterfaces.ITraversable)
	class BrokenTraversable(object):
		def traverse( self, name, furtherPath ):
			getattr( self, u'\u2019', None )


	def is_api_broken():
		try:
			_zapi.traverseName( BrokenTraversable(), '' )
		except UnicodeEncodeError:
			return True
		except _zadapters.LocationError:
			return False

	if is_api_broken():
		logger.info( "Monkey patching zope.traversing to be robust to unicode attr names" )

		_zadapters.traversePathElement.__code__ = _patched_traversePathElement.__code__
		_zadapters.traversePathElement.func_globals['_nti_exc_info'] = sys.exc_info
		_zadapters._nti_traversePathElement = _nti_traversePathElement

		global _patched_traversing
		_patched_traversing = True
	assert not is_api_broken(), "Patched it"

	# Now, is the default adapter broken?
	# Note that zope.container.traversal.ContainerTraversable handles this correctly,
	# but it has the order backwards from the DefaultTraversable.
	def is_adapter_broken():
		try:
			_zadapters.DefaultTraversable( object() ).traverse( u'\u2019', () )
		except UnicodeEncodeError:
			return True
		except _zadapters.LocationError:
			return False

	if is_adapter_broken():
		# Sadly, the best thing to do is replace this entirely
		LocationError = _zadapters.LocationError
		def fixed_traverse( self, name, furtherPath ):
			subject = self._subject
			__traceback_info__ = subject, name, furtherPath
			try:
				attr = getattr( subject, name, _marker )
			except UnicodeEncodeError:
				attr = _marker
			if attr is not _marker:
				return attr
			if hasattr(subject, '__getitem__'):
				try:
					return subject[name]
				except (KeyError, TypeError):
					pass
			raise LocationError(subject, name)

		_zadapters.DefaultTraversable.traverse = fixed_traverse
	assert not is_adapter_broken()


_patch_traversing()

del _zinterface
del _zapi
del _zadapters
del _zinterfaces

#del logger
del logging
