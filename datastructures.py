#!/usr/bin/env python2.7
"""
Datatypes and datatype handling.
$Revision$
"""
# TODO: Split this apart, move externalization to its own module.

import logging
logger = logging.getLogger( __name__ )

import time
import collections
import UserList
import weakref

import persistent
import BTrees.OOBTree
import ZODB

import plistlib
import json

import six
import numbers

from zope import interface
from zope import component


from .interfaces import (IHomogeneousTypeContainer, IHTC_NEW_FACTORY,
						 IExternalObject,
						 ILocation)
from . import links
from . import ntiids
from . import mimetype
from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver import authorization_acl as nacl

__all__ = ['toExternalObject', 'ModDateTrackingObject', 'ExternalizableDictionaryMixin',
		   'CreatedModDateTrackingObject', 'ModDateTrackingMappingMixin', 'ModDateTrackingOOBTree',
		   'ModDateTrackingPersistentMapping', 'PersistentExternalizableDictionary', 'CreatedModDateTrackingPersistentMapping',
		   'PersistentExternalizableList', 'IDItemMixin', 'PersistentCreatedModDateTrackingObject', 'LocatedExternalDict',
		   'getPersistentState', 'setPersistentStateChanged', 'IExternalObject', 'isSyntheticKey', 'AbstractNamedContainerMap',
		   'ContainedMixin', 'toExternalOID', 'fromExternalOID',  'stripNoneFromExternal', 'stripSyntheticKeysFromExternalDictionary',
		   'ContainedStorage', 'LastModifiedCopyingUserList', 'ExternalizableInstanceDict',
		   'to_external_representation', 'to_json_representation', 'EXT_FORMAT_JSON', 'EXT_FORMAT_PLIST',
		   'StandardExternalFields', 'StandardInternalFields', 'toExternalDictionary',
		   'to_external_ntiid_oid' ]

from zope.container._zope_container_contained import isProxy as _isContainedProxy
from zope.container._zope_container_contained import getProxiedObject as _getContainedProxiedObject

def getPersistentState( obj ):
	""" For a Persistent object, returns one of the
	constants from the persistent module for its state:
	CHANGED and UPTODATE being the most useful. If the object
	is not Persistent and doesn't implement a 'getPersistentState' method,
	this method will be pessimistic and assume the object has
	been CHANGED."""
	if hasattr( obj, '_p_changed' ):
		if getattr(obj, '_p_changed', False ):
			# Trust the changed value ahead of the state value,
			# because it is settable from python but the state
			# is more implicit.
			return persistent.CHANGED
		if getattr( obj, '_p_state', -1 ) == persistent.UPTODATE and getattr( obj, '_p_jar', -1 ) is None:
			# In keeping with the pessimistic theme, if it claims to be uptodate, but has never
			# been saved, we consider that the same as changed
			return persistent.CHANGED
		# We supply container classes that wrap objects (that are not IContained/ILocation)
		# in ContainerProxy classes. The proxy doesn't proxy _p_changed, which
		# leads to weird behaviour for things that want to notice changes (users.User.endUpdates)
		# so we need to reflect those changes to the actual object ourself
		# TODO: Such places should be using events
		if _isContainedProxy(obj):
			return getPersistentState( _getContainedProxiedObject( obj ) )
		return persistent.UPTODATE
	if hasattr(obj, '_p_state'):
		return getattr(obj, '_p_state' )
	if hasattr( obj, 'getPersistentState' ):
		return obj.getPersistentState()
	return persistent.CHANGED

def setPersistentStateChanged( obj ):
	""" Explicitly marks a persistent object as changed. """
	if hasattr(obj, '_p_changed' ):
		setattr(obj, '_p_changed', True )

def toExternalOID( self, default=None ):
	""" For a persistent object, returns its persistent OID in a pasreable
	external format. If the object has not been saved, returns the default. """
	oid = default
	if hasattr( self, 'toExternalOID' ):
		oid = self.toExternalOID( )
	elif hasattr(self, '_p_oid') and getattr(self, '_p_oid'):
		# The object ID is defined to be 8 charecters long. It gets
		# padded with null chars to get to that length; we strip
		# those out. Finally, it probably has chars that
		# aren't legal it UTF or ASCII, so we go to hex and prepend
		# a flag, '0x'
		oid = getattr(self, '_p_oid').lstrip('\x00')
		oid = '0x' + oid.encode('hex')
		if hasattr(self, '_p_jar') and getattr(self, '_p_jar'):
			db_name = self._p_jar.db().database_name
			oid = oid + ':' + db_name.encode( 'hex' )
	return oid

def fromExternalOID( ext_oid ):
	"""
	:return: A tuple of OID, database name. Name may be empty.
	:param string ext_oid: As produced by :func:`toExternalOID`.
	"""
	oid_string, name_s = ext_oid.split( ':' ) if ':' in ext_oid else (ext_oid, "")
	# Translate the external format if needed
	if oid_string.startswith( '0x' ):
		oid_string = oid_string[2:].decode( 'hex' )
		name_s = name_s.decode( 'hex' )
	# Recall that oids are padded to 8 with \x00
	oid_string = oid_string.rjust( 8, '\x00' )
	return oid_string, name_s

_ext_ntiid_oid = object()
def to_external_ntiid_oid( contained, default_oid=_ext_ntiid_oid ):
	"""
	:return: An NTIID string utilizing the object's creator and persistent
		id.
	:param default_oid: The default value for the externalization of the OID.
		If this is None, and no external OID can be found (using :func:`toExternalOID`),
		then this function will return None.
	"""
	# We really want the external OID, but for those weird time we may not be saved we'll
	# allow the ID of the object, unless we are explicitly overridden
	oid = toExternalOID( contained, default=(default_oid if default_oid is not _ext_ntiid_oid else str(id(contained))) )
	if not oid:
		return None

	creator = getattr( contained, 'creator', nti_interfaces.SYSTEM_USER_NAME )
	return ntiids.make_ntiid( provider=(creator
										if isinstance( creator, six.string_types )
										else getattr( creator, 'username', nti_interfaces.SYSTEM_USER_NAME )),
								specific=oid,
								nttype=ntiids.TYPE_OID )

# It turns out that the name we use for externalization (and really the registry, too)
# we must keep thread-local. We call into objects without any context,
# and they call back into us, and otherwise we would lose
# the name that was established at the top level.
_ex_name_marker = object()
import gevent.local
class _ex_name_local_c(gevent.local.local):
	def __init__( self ):
		super(_ex_name_local_c,self).__init__()
		self.name = [_ex_name_marker]
_ex_name_local = _ex_name_local_c
_ex_name_local.name = [_ex_name_marker]

def toExternalObject( obj, coerceNone=False, name=_ex_name_marker, registry=component ):
	""" Translates the object into a form suitable for
	external distribution, through some data formatting process.

	:param string name: The name of the adapter to :class:IExternalObject to look
		for. Defaults to the empty string (the default adapter). If you provide
		a name, and an adapter is not found, we will still look for the default name
		(unless the name you supply is None).

	"""

	if isinstance( obj, six.string_types ) or isinstance( obj, numbers.Number ):
		return obj

	if name == _ex_name_marker:
		name = _ex_name_local.name[-1]
	if name == _ex_name_marker:
		name = ''
	_ex_name_local.name.append( name )

	try:
		def recall( obj ):
			return toExternalObject( obj, coerceNone=coerceNone, name=name, registry=registry )

		if not IExternalObject.providedBy( obj ) and not hasattr( obj, 'toExternalObject' ):
			adapter = registry.queryAdapter( obj, IExternalObject, default=None, name=name )
			if not adapter and name == '':
				# try for the default, but allow passing name of None to disable
				adapter = registry.queryAdapter( obj, IExternalObject, default=None, name='' )
			# if not adapter and name == '':
			# 	# try for the default, but allow passing name of None to disable
			# 	adapter = registry.queryAdapter( obj, IExternalObject, default=None, name='wsgi' )
			if adapter:
				obj = adapter

		result = obj
		if hasattr( obj, "toExternalObject" ):
			result = obj.toExternalObject()
		elif hasattr( obj, "toExternalDictionary" ):
			result = obj.toExternalDictionary()
		elif hasattr( obj, "toExternalList" ):
			result = obj.toExternalList()
		elif isinstance(obj, (persistent.mapping.PersistentMapping,BTrees.OOBTree.OOBTree,collections.Mapping)):
			result = toExternalDictionary( obj, name=name, registry=registry )
			if obj.__class__ == dict: result.pop( 'Class', None )
			for key, value in obj.iteritems():
				result[key] = recall( value )
		elif isinstance( obj, (persistent.list.PersistentList, collections.Set, list) ):
			result = LocatedExternalList( [recall(x) for x in obj] )
		# PList doesn't support None values, JSON does. The closest
		# coersion I can think of is False.
		elif obj is None and coerceNone:
			result = False
		elif isinstance( obj, ZODB.broken.PersistentBroken ):
			# Broken objects mean there's been a persistence
			# issue
			logger.debug("Broken object found %s, %s", type(obj), obj)
			result = 'Broken object'

		return result
	finally:
		_ex_name_local.name.pop()


def stripNoneFromExternal( obj ):
	""" Given an already externalized object, strips None values. """
	if isinstance( obj, list ) or isinstance(obj, tuple):
		obj = [stripNoneFromExternal(x) for x in obj if x is not None]
	elif isinstance( obj, collections.Mapping ):
		obj = {k:stripNoneFromExternal(v)
			   for k,v in obj.iteritems()
			   if (v is not None and k is not None)}
	return obj

def stripSyntheticKeysFromExternalDictionary( external ):
	""" Given a mutable dictionary, removes all the external keys
	that might have been added by toExternalDictionary and echoed back. """
	for key in _syntheticKeys():
		external.pop( key, None )
	return external

EXT_FORMAT_JSON = 'json'
EXT_FORMAT_PLIST = 'plist'

def to_external_representation( obj, ext_format=EXT_FORMAT_PLIST, name=_ex_name_marker, registry=component ):
	"""
	Transforms (and returns) the `obj` into its external (string) representation.

	:param ext_format: One of :const:EXT_FORMAT_JSON or :const:EXT_FORMAT_PLIST.
	"""
	# It would seem nice to be able to do this in one step during
	# the externalization process itself, but we would wind up traversing
	# parts of the datastructure more than necessary. Here we traverse
	# the whole thing exactly twice.
	ext = toExternalObject( obj, name=name, registry=registry )

	if ext_format == EXT_FORMAT_PLIST:
		ext = stripNoneFromExternal( ext )
		try:
			ext = plistlib.writePlistToString( ext )
		except TypeError:
			logger.exception( "Failed to externalize %s", ext )
			raise
	else:
		ext = json.dumps( ext )
	return ext

def to_json_representation( obj ):
	""" A convenience function that calls :func:`to_external_representation` with :data:`EXT_FORMAT_JSON`."""
	return to_external_representation( obj, EXT_FORMAT_JSON )

def _weakRef_toExternalObject(self):
	val = self()
	if val is None:
		return None
	return toExternalObject( val )

persistent.wref.WeakRef.toExternalObject = _weakRef_toExternalObject

def _weakRef_toExternalOID(self):
	val = self()
	if val is None:
		return None
	return toExternalOID( val )

persistent.wref.WeakRef.toExternalOID = _weakRef_toExternalOID


class ModDateTrackingObject(object):
	""" Maintains an lastModified attribute containing a time.time()
	modification stamp. Use updateLastMod() to update this value. """

	__conflict_max_keys__ = ['lastModified']
	__conflict_merge_keys__ = []

	def __init__( self, *args, **kwargs ):
		super(ModDateTrackingObject,self).__init__( *args, **kwargs )
		self._lastModified = 0

	def __setstate__( self, state ):
		if state and 'lastModified' in state:
			state['_lastModified'] = state['lastModified']
			del state['lastModified']
		super(ModDateTrackingObject,self).__setstate__( state )

	def _get_lastModified(self):
		# To make it easy to add this class as a mixin
		# to any class, some of which may already be in the
		# database, we handle missing last modified values
		try:
			return self._lastModified
		except AttributeError:
			return 0
	def _set_lastModified(self, lm):
		self._lastModified = lm
	lastModified = property( _get_lastModified, _set_lastModified )

	def updateLastMod(self, t=None ):
		self.lastModified = t if t is not None and t > self.lastModified else time.time()
		return self.lastModified

	def updateLastModIfGreater( self, t ):
		"Only if the given time is (not None and) greater than this object's is this object's time changed."
		if t is not None and t > self.lastModified:
			self.lastModified = t
		return self.lastModified

	def _p_resolveConflict(self, oldState, savedState, newState):
		logger.warn( 'Conflict to resolve in %s:\n\t%s\n\t%s\n\t%s', type(self), oldState, savedState, newState )
		# TODO: This is not necessarily safe here.
		for k in newState:
			# cannot count on keys being both places
			if savedState.get(k) != newState.get(k):
				logger.info( "%s\t%s\t%s", k, savedState[k], newState[k] )

		d = savedState # Start with saved state, don't lose any changes already committed.
		for k in self.__conflict_max_keys__:
			d[k] = max( oldState[k], savedState[k], newState[k] )
			logger.warn( "New value for %s:\t%s", k, d[k] )

		for k in self.__conflict_merge_keys__:
			saveDiff = savedState[k] - oldState[k]
			newDiff = newState[k] - oldState[k]
			d[k] = oldState[k] + saveDiff + newDiff
			logger.warn( "New value for %s:\t%s", k, d[k] )
		return d

def _syntheticKeys( ):
	return ('OID', 'ID', 'Last Modified', 'Creator', 'ContainerId', 'Class')

def _isMagicKey( key ):
	""" For our mixin objects that have special keys, defines
	those keys that are special and not settable by the user. """
	return key in _syntheticKeys()

isSyntheticKey = _isMagicKey

class StandardExternalFields(object):

	OID   = 'OID'
	ID    = 'ID'
	NTIID = 'NTIID'
	LAST_MODIFIED = 'Last Modified'
	CREATED_TIME = 'CreatedTime'
	CREATOR = 'Creator'
	CONTAINER_ID = 'ContainerId'
	CLASS = 'Class'
	MIMETYPE = 'MimeType'
	LINKS = 'Links'
	HREF = 'href'

StandardExternalFields.ALL = [ v for k,v in StandardExternalFields.__dict__.iteritems() if not k.startswith( '_' ) ]


class StandardInternalFields(object):
	ID = 'id'
	NTIID = 'ntiid'

	CREATOR = 'creator'
	LAST_MODIFIED = 'lastModified'
	LAST_MODIFIEDU = 'LastModified'
	CREATED_TIME = 'createdTime'
	CONTAINER_ID = 'containerId'

class LocatedExternalDict(dict):
	"""
	A dictionary that implements ILocation. Returned
	by toExternalDictionary.
	"""
	interface.implements( ILocation )
	__name__ = ''
	__parent__ = None
	__acl__ = ()

class LocatedExternalList(list):
	"""
	A list that implements ILocation. Returned
	by toExternalObject.
	"""
	interface.implements( ILocation )
	__name__ = ''
	__parent__ = None
	__acl__ = ()

def toExternalDictionary( self, mergeFrom=None, name=_ex_name_marker, registry=component):
	""" Returns a dictionary of the object's contents. The super class's
	implementation MUST be called and your object's values added to it.
	This impl takes care of adding the standard attributes including
	OID (from self._p_oid) and ID (from self.id if defined) and
	Creator (from self.creator).

	For convenience, if mergeFrom is not None, then those values will
	be added to the dictionary created by this method. This allows a pattern like:
	def toDictionary(self): return super(MyClass,self).toDictionary( {'key': self.val } )
	The keys and values in mergeFrom should already be external.
	"""
	result = LocatedExternalDict()
	result.__acl__ = nacl.ACL( self )
	if mergeFrom:
		result.update( mergeFrom )

	def _ordered_pick( ext_name, *fields ):
		for x in fields:
			if isinstance( x, basestring) and getattr( self, x, ''):
				result[ext_name] = getattr( self, x )
				if callable( fields[-1] ):
					result[ext_name] = fields[-1]( result[ext_name] )
				break

	_ordered_pick( StandardExternalFields.ID, StandardInternalFields.ID, StandardExternalFields.ID )
	# As we transition over to structured IDs that contain OIDs, we'll try to use that
	# for both the ID and OID portions
	if ntiids.is_ntiid_of_type( result.get( StandardExternalFields.ID ), ntiids.TYPE_OID ):
		result[StandardExternalFields.OID] = result[StandardExternalFields.ID]
	else:
		oid = to_external_ntiid_oid( self, default_oid=None ) #toExternalOID( self )
		if oid:
			result[StandardExternalFields.OID] = oid

	_ordered_pick( StandardExternalFields.CREATOR, StandardInternalFields.CREATOR, StandardExternalFields.CREATOR, str )
	_ordered_pick( StandardExternalFields.LAST_MODIFIED, StandardInternalFields.LAST_MODIFIED, StandardInternalFields.LAST_MODIFIEDU )
	_ordered_pick( StandardExternalFields.CREATED_TIME, StandardInternalFields.CREATED_TIME )


	if hasattr( self, '__external_class_name__' ):
		result[StandardExternalFields.CLASS] = getattr( self, '__external_class_name__' )
	elif self.__class__.__module__ != ExternalizableDictionaryMixin.__module__ \
		   and not self.__class__.__name__.startswith( '_' ):
		result[StandardExternalFields.CLASS] = self.__class__.__name__

	_ordered_pick( StandardExternalFields.CONTAINER_ID, StandardInternalFields.CONTAINER_ID )
	_ordered_pick( StandardExternalFields.NTIID, StandardInternalFields.NTIID, StandardExternalFields.NTIID )
	# During the transition, if there is not an NTIID, but we can find one as the ID or OID,
	# provide that
	if StandardExternalFields.NTIID not in result:
		for field in (StandardExternalFields.ID,StandardExternalFields.OID):
			if ntiids.is_valid_ntiid_string( result.get( field ) ):
				result[StandardExternalFields.NTIID] = result[field]
				break

	if StandardExternalFields.CLASS in result:
		mime_type = mimetype.nti_mimetype_from_object( self, use_class=False )
		if mime_type:
			result[StandardExternalFields.MIMETYPE] = mime_type

	# Links.
	# TODO: This needs to be all generalized. Howso?
	_links = find_links(self)
	_links = [toExternalObject(l,name=name,registry=registry) for l in _links if l]
	_links = [l for l in _links if l]
	if _links:
		for link in _links:
			interface.alsoProvides( link, ILocation )
			link.__name__ = ''
			link.__parent__ = self
		result[StandardExternalFields.LINKS] = _links

	return result

def find_links( self ):
	"""
	Return a sequence of things that should be thought of as related links to
	a given object, including enclosures and the `links` property.
	:return: A sequence of :class:`interfaces.ILink` objects.
	"""
	_links = []
	if callable( getattr( self, 'iterenclosures', None ) ):
		_links = [links.Link(enclosure,rel='enclosure')
				  for enclosure
				  in self.iterenclosures()]
	_links.extend( getattr( self, 'links', () ) )
	return _links


class ExternalizableDictionaryMixin(object):
	""" Implements a toExternalDictionary method as a base for subclasses. """

	def __init__(self, *args):
		super(ExternalizableDictionaryMixin,self).__init__(*args)

	def toExternalDictionary( self, mergeFrom=None):
		return toExternalDictionary( self, mergeFrom=mergeFrom )

	def stripSyntheticKeysFromExternalDictionary( self, external ):
		""" Given a mutable dictionary, removes all the external keys
		that might have been added by toExternalDictionary and echoed back. """
		for k in _syntheticKeys():
			external.pop( k, None )
		return external

class ExternalizableInstanceDict(ExternalizableDictionaryMixin):
	"""Externalizes to a dictionary containing the members of __dict__ that do not start with an underscore."""
	interface.implements(IExternalObject)
	# TODO: there should be some better way to customize this if desired (an explicit list)
	# TODO: Play well with __slots__
	# TODO: This won't evolve well. Need something more sophisticated,
	# probably a meta class.

	# Avoid things super handles
	_excluded_out_ivars_ = {StandardInternalFields.ID, StandardExternalFields.ID, StandardInternalFields.CREATOR,
							StandardExternalFields.CREATOR, StandardInternalFields.CONTAINER_ID,
							'lastModified', StandardInternalFields.LAST_MODIFIEDU, StandardInternalFields.CREATED_TIME,
							'links'}
	_excluded_in_ivars_ = {StandardInternalFields.ID, StandardExternalFields.ID,
						   StandardExternalFields.OID,
						   StandardInternalFields.CREATOR,
						   StandardExternalFields.CREATOR,
						   'lastModified',
						   StandardInternalFields.LAST_MODIFIEDU,
						   StandardExternalFields.CLASS,
						   StandardInternalFields.CONTAINER_ID}
	_prefer_oid_ = False

	def toExternalDictionary( self, mergeFrom=None ):
		result = super(ExternalizableInstanceDict,self).toExternalDictionary( mergeFrom=mergeFrom )
		for k in self.__dict__:
			if (k not in self._excluded_out_ivars_  # specifically excluded
				and not k.startswith( '_' )			# private
				and not k in result					# specifically given
				and not callable(getattr(self,k))):	# avoid functions

				result[k] = toExternalObject( getattr( self, k ) )
				if ILocation.providedBy( result[k] ):
					result[k].__parent__ = self
		if StandardExternalFields.ID in result and StandardExternalFields.OID in result \
			   and self._prefer_oid_ and result[StandardExternalFields.ID] != result[StandardExternalFields.OID]:
			result[StandardExternalFields.ID] = result[StandardExternalFields.OID]
		return result

	def toExternalObject( self, mergeFrom=None ):
		return self.toExternalDictionary(mergeFrom)

	def updateFromExternalObject( self, parsed, *args, **kwargs ):
		for k in parsed:
			if k in self.__dict__ and k not in self._excluded_in_ivars_:
				setattr( self, k, parsed[k] )

		if StandardExternalFields.CONTAINER_ID in parsed and getattr( self, StandardInternalFields.CONTAINER_ID, parsed ) is None:
			setattr( self, StandardInternalFields.CONTAINER_ID, parsed[StandardExternalFields.CONTAINER_ID] )
		if StandardExternalFields.CREATOR in parsed and getattr( self, StandardExternalFields.CREATOR, parsed ) is None:
			setattr( self, StandardExternalFields.CREATOR, parsed[StandardExternalFields.CREATOR] )

	def __repr__( self ):
		try:
			return "%s().__dict__.update( %s )" % (self.__class__.__name__, self.toExternalDictionary() )
		except ZODB.POSException.ConnectionStateError:
			return '%s(Ghost)' % self.__class__.__name__
		except ValueError as e: # Things like invalid NTIID
			return '%s(%s)' % (self.__class__.__name__, e)


class CreatedModDateTrackingObject(ModDateTrackingObject):
	""" Adds the `creator` and `createdTime` attributes. """
	def __init__( self, *args ):
		super(CreatedModDateTrackingObject,self).__init__( *args )
		self.creator = None
		self.createdTime = time.time()

class PersistentCreatedModDateTrackingObject(persistent.Persistent,CreatedModDateTrackingObject):
	pass

class ModDateTrackingMappingMixin(CreatedModDateTrackingObject):

	def __init__( self, *args ):
		super(ModDateTrackingMappingMixin, self).__init__( *args )

	def updateLastMod(self, t=None ):
		ModDateTrackingObject.updateLastMod( self, t )
		super(ModDateTrackingMappingMixin,self).__setitem__(StandardExternalFields.LAST_MODIFIED, self.lastModified )
		return self.lastModified

	def __delitem__(self, key):
		if _isMagicKey( key ):
			return
		super(ModDateTrackingMappingMixin, self).__delitem__(key)
		self.updateLastMod()

	def __setitem__(self, key, y):
		if _isMagicKey( key ):
			return

		super(ModDateTrackingMappingMixin, self).__setitem__(key,y)
		self.updateLastMod()

	def update( self, d ):
		super(ModDateTrackingMappingMixin, self).update( d )
		self.updateLastMod()

	def pop( self, key, *args ):
		result = super(ModDateTrackingMappingMixin, self).pop( key, *args )
		self.updateLastMod()
		return result

	def popitem( self ):
		result = super(ModDateTrackingMappingMixin, self).popitem()
		self.updateLastMod()
		return result

class ModDateTrackingOOBTree(ModDateTrackingMappingMixin, BTrees.OOBTree.OOBTree, ExternalizableDictionaryMixin):

	def __init__(self, *args):
		super(ModDateTrackingOOBTree,self).__init__(*args)

	def toExternalDictionary(self, mergeFrom=None):
		result = super(ModDateTrackingOOBTree,self).toExternalDictionary(mergeFrom)
		for key, value in self.iteritems():
			result[key] = toExternalObject( value )
		return result

	def _p_resolveConflict(self, oldState, savedState, newState ):
		logger.info( 'Conflict to resolve in %s', type(self) )
		# Our super class will generally resolve what conflicts it
		# can, or throw an exception. If it resolves things,
		# we just want to update our last modified time---that's the thing
		# most likely to conflict
		result = dict( super(ModDateTrackingOOBTree,self)._p_resolveConflict( oldState, savedState, newState ) )
		result['lastModified'] = max( oldState['lastModified'], savedState['lastModified'], newState['lastModified'] )
		return result

import functools
@functools.total_ordering
class _CaseInsensitiveKey(object):

	def __init__( self, key ):
		self.key = key
		self._lower_key = key.lower()

	def __str__( self ):
		return self.key

	def __repr__( self ):
		return "%s('%s')" % (self.__class__, self.key)

	def __eq__(self, other):
		return other != None and self._lower_key == getattr(other, '_lower_key', None)

	def __lt__(self, other):
		return self._lower_key < getattr(other, '_lower_key', other)

class CaseInsensitiveModDateTrackingOOBTree(ModDateTrackingOOBTree):
	"""
	This class should not be used as it changes the stored keys.
	"""
	# This is left for backwards compatibility.

	def __init__(self, *args ):
		super(CaseInsensitiveModDateTrackingOOBTree, self).__init__( *args )

	def _tx_key( self, key ):
		# updateLastModified doesn't go through our transformation,
		# so we must also not transform the Last Modified key
		if not _isMagicKey( key ) and isinstance( key, six.string_types ):
			key = key.lower()
		return key

	def __getitem__(self, key):
		key = self._tx_key( key )
		return super(CaseInsensitiveModDateTrackingOOBTree, self).__getitem__(key)

	def __contains__(self, key):
		key = self._tx_key( key )
		return super(CaseInsensitiveModDateTrackingOOBTree, self).__contains__(key)

	def __delitem__(self, key):
		key = self._tx_key( key )
		return super(CaseInsensitiveModDateTrackingOOBTree, self).__delitem__(key)

	def __setitem__(self, key, value):
		key = self._tx_key( key )
		return super(CaseInsensitiveModDateTrackingOOBTree, self).__setitem__(key, value)

	def get( self, key, dv=None ):
		key = self._tx_key( key )
		return super( CaseInsensitiveModDateTrackingOOBTree, self).get( key, dv )

class KeyPreservingCaseInsensitiveModDateTrackingOOBTree(CaseInsensitiveModDateTrackingOOBTree):
	"""
	Preserves the case of the inserted keys.
	"""

	def __init__(self, *args ):
		super(KeyPreservingCaseInsensitiveModDateTrackingOOBTree, self).__init__( *args )

	def _tx_key( self, key ):
		if not _isMagicKey( key ) and isinstance( key, six.string_types ):
			key = _CaseInsensitiveKey( key )
		return key

	def keys(self,key=None):
		# TODO: Don't force this to be materialized
		return [getattr(k,'key',k) for k in super(KeyPreservingCaseInsensitiveModDateTrackingOOBTree,self).keys(key)]

	def items(self,key=None):
		# TODO: Don't force this to be materialized
		return [(getattr(k,'key',k),v) for k,v in super(KeyPreservingCaseInsensitiveModDateTrackingOOBTree,self).items(key)]

	def iterkeys(self,key=None):
		return (getattr(k,'key',k) for k in super(KeyPreservingCaseInsensitiveModDateTrackingOOBTree,self).keys(key))

	__iter__ = iterkeys

	def iteritems(self):
		return ((getattr(k,'key',k),v) for k,v in super(KeyPreservingCaseInsensitiveModDateTrackingOOBTree,self).items())

collections.Mapping.register( BTrees.OOBTree.OOBTree )

class ModDateTrackingPersistentMapping(ModDateTrackingMappingMixin, persistent.mapping.PersistentMapping, ExternalizableDictionaryMixin):

	def __init__(self, *args, **kwargs):
		super(ModDateTrackingPersistentMapping,self).__init__(*args, **kwargs)
		# Copy in the creator and last modified from the first argument
		# (the initial data) if it has them and we don't yet have them
		if args:
			if getattr( args[0], StandardInternalFields.CREATOR, None ) and not self.creator:
				self.creator = args[0].creator
			if getattr( args[0], 'lastModified', None ) and not self.lastModified:
				self.lastModified = args[0].lastModified


	def toExternalDictionary(self, mergeFrom=None):
		result = super(ModDateTrackingPersistentMapping,self).toExternalDictionary(mergeFrom)
		for key, value in self.iteritems():
			result[key] = toExternalObject( value )
		return result

	def __hash__( self ):
		return hash( tuple( self.iterkeys() ) )

CreatedModDateTrackingPersistentMapping = ModDateTrackingPersistentMapping

class PersistentExternalizableDictionary(persistent.mapping.PersistentMapping,ExternalizableDictionaryMixin):

	def __init__(self, dict=None, **kwargs ):
		super(PersistentExternalizableDictionary, self).__init__( dict, **kwargs )

	def toExternalDictionary( self, mergeFrom=None):
		result = super(PersistentExternalizableDictionary,self).toExternalDictionary( self )
		for key, value in self.iteritems():
			result[key] = toExternalObject( value )
		return result

class PersistentExternalizableList(ModDateTrackingObject,persistent.list.PersistentList):

	def __init__(self, initlist=None):
		# Must use new-style super call to get right behaviour
		super(PersistentExternalizableList,self).__init__(initlist)

	def toExternalList( self ):
		result = [toExternalObject(x) for x in self if x is not None]
		return result

class LastModifiedCopyingUserList(ModDateTrackingObject,UserList.UserList):
	""" For building up a sequence of lists, keeps the max last modified. """
	def extend( self, other ):
		super(LastModifiedCopyingUserList,self).extend( other )
		self.updateLastModIfGreater( getattr( other, 'lastModified', self.lastModified ) )

	def __iadd__( self, other ):
		result = super(LastModifiedCopyingUserList,self).__iadd__( other )
		self.updateLastModIfGreater( getattr( other, 'lastModified', self.lastModified ) )
		return result

	def toExternalObject(self):
		return self.data

from persistent.wref import WeakRef

class PersistentExternalizableWeakList(PersistentExternalizableList):
	"""
	Stores :class:`persistent.Persistent` objects as weak references, invisibly to the user.
	Any weak references added to the list will be treated the same.
	"""

	def __getitem__(self, i ):
		return super(PersistentExternalizableWeakList,self).__getitem__( i )()

	# __iter__ is implemented with __getitem__. However, __eq__ isn't, it wants
	# to directly compare lists
	def __eq__( self, other ):
		# If we just compare lists, weak refs will fail badly
		# if they're compared with non-weak refs
		if not isinstance( other, collections.Sequence ):
			return False

		result = False
		if len(self) == len(other):
			result = True
			for i in xrange(len(self)):
				if self[i] != other[i]:
					result = False
					break
		return result

	def __wrap( self, obj ):
		return obj if isinstance( obj, WeakRef ) else WeakRef( obj )


	def remove(self,value):
		super(PersistentExternalizableWeakList,self).remove( self.__wrap( WeakRef(value) ) )
		self.updateLastMod()

	def __setitem__(self, i, item):
		super(PersistentExternalizableWeakList,self).__setitem__( i, self.__wrap( WeakRef( item ) ) )
		self.updateLastMod()

	def __setslice__(self, i, j, other):
		raise TypeError( 'Not supported' )

	# Unfortunately, these are not implemented in terms of the primitives, so
	# we need to overide each one. They can throw exceptions, so we're careful
	# not to prematurely update lastMod

	def __iadd__(self, other):
		# We must wrap each element in a weak ref
		# Note that the builtin list only accepts other lists,
		# but the UserList from which we are descended accepts
		# any iterable.
		result = super(PersistentExternalizableWeakList,self).__iadd__([self.__wrap(WeakRef(o)) for o in other])
		self.updateLastMod()
		return result

	def __imul__(self, n):
		result = super(PersistentExternalizableWeakList,self).__imul__(n)
		self.updateLastMod()
		return result

	def append(self, item):
		super(PersistentExternalizableWeakList,self).append(self.__wrap( WeakRef(item) ) )
		self.updateLastMod()

	def insert(self, i, item):
		super(PersistentExternalizableWeakList,self).insert( i, self.__wrap( WeakRef(item)) )
		self.updateLastMod()

	def pop(self, i=-1):
		rtn = super(PersistentExternalizableWeakList,self).pop( i )
		self.updateLastMod()
		return rtn()

	def extend(self, other):
		for x in other: self.append( x )

	def count( self, item ):
		return super(PersistentExternalizableWeakList,self).count( self.__wrap( WeakRef( item ) ) )

	def index( self, item, *args ):
		return super(PersistentExternalizableWeakList,self).index( self.__wrap( WeakRef( item ) ), *args )

class IDItemMixin(object):
	def __init__(self):
		super(IDItemMixin,self).__init__()
		self.id = None

	def __setitem__(self, key, value ):
		if key == StandardExternalFields.ID:
			self.id = value
		else:
			super(IDItemMixin,self).__setitem__(key,value)

	def __getitem__(self, key):
		if key == StandardExternalFields.ID: return self.id
		return super(IDItemMixin,self).__getitem__(key)

class ContainedMixin(object):
	""" Defines something that can be logically contained inside another unit
	by reference. Two properties are defined, id and containerId. """

	interface.implements( nti_interfaces.IContained )

	def __init__(self, containerId=None, containedId=None):
		super(ContainedMixin,self).__init__()
		self.containerId = containerId
		self.id = containedId

import zope.container.btree

class ModDateTrackingBTreeContainer(zope.container.btree.BTreeContainer):
	interface.implements( nti_interfaces.ILastModified )

	def __init__( self ):
		super(ModDateTrackingBTreeContainer,self).__init__()

	def _newContainerData(self):
		return ModDateTrackingOOBTree()

	def __len__( self ):
		# The 'last modified' member is always present
		# and implicitly added
		return super(ModDateTrackingBTreeContainer,self).__len__() + 1

	@property
	def lastModified(self):
		return self._SampleContainer__data.lastModified

	def updateLastMod(self, t=None ):
		return self._SampleContainer__data.updateLastMod( t=t )

	def updateLastModIfGreater( self, t ):
		return self._SampleContainer__data.updateLastModIfGreater( t )

	def itervalues(self):
		return self._SampleContainer__data.itervalues()

	def iterkeys(self):
		return self._SampleContainer__data.iterkeys()

	def iteritems(self):
		return self._SampleContainer__data.iteritems()

collections.Mapping.register( ModDateTrackingBTreeContainer )

class KeyPreservingCaseInsensitiveModDateTrackingBTreeContainer(ModDateTrackingBTreeContainer):

	def _newContainerData(self):
		return KeyPreservingCaseInsensitiveModDateTrackingOOBTree()

def _noop(*args): pass

class ContainedStorage(persistent.Persistent,ModDateTrackingObject):
	"""
	A specialized data structure for tracking contained objects.
	"""

	####
	# Conflict Resolution:
	# All the properties of this class itself are read-only,
	# with the exception of self.lastModified. Our containers map
	# is an OOBTree, which itself resolves conflicts. Therefore,
	# to resolve conflicts, we only need to take the attributes
	# from newState (the only thing that would have changed
	# is last modified), updating lastModified to now.
	####

	def __init__( self, weak=False, create=False, containers=None, containerType=ModDateTrackingBTreeContainer,
				  set_ids=True, containersType=ModDateTrackingOOBTree):
		"""
		Creates a new container.

		:param bool weak: If true, we will maintain weak references to contained objects.
		:param object create: A boolean or object value. If it is true, the `creator` property
			of objects added to us will be set. If `create` is a boolean, this `creator` property
			will be set to this object (useful for subclassing). Otherwise, the `creator` property
			will be set to the value of `create`.
		:param dict containers: Initial containers
		:param type containerType: The type for each created container. Should be a mapping
			type, and should handle conflicts. The default value only allows comparable keys.
		:param type containersType: The mapping type factory that will hold the containers.
			Default is :class:`ModDateTrackingOOBTree`, another choice is :class:`CaseInsensitiveModDateTrackingOOBTree`.
		:param bool set_ids: If true (default) the ``id`` field of newly added objects will be set.
			Otherwise, the must already have an id. Set this to False if the added objects
			are shared (and especially shared in the database.)
		"""
		super(ContainedStorage,self).__init__()
		self.containers = containersType() # read-only, but mutates.
		self.weak = weak # read-only
		self.create = create # read-only
		self.containerType = containerType # read-only
		self.set_ids = set_ids # read-only
		self._setup( )

		for k,v in (containers or {}).iteritems():
			self.containers[k] = v

	def _setup( self ):
		if self.weak:
			def wrap(obj):
				return WeakRef( obj ) if hasattr( obj, '_p_oid' ) else weakref.ref( obj )
			def unwrap(obj):
				return obj() if obj is not None else None
			self._v_wrap = wrap
			self._v_unwrap = unwrap
		else:
			def wrap(obj): return obj
			def unwrap(obj): return obj
			self._v_wrap = wrap
			self._v_unwrap = unwrap

		if self.create:
			creator = self if isinstance(self.create, bool) else self.create
			def _create(obj):
				obj.creator = creator
			self._v_create = _create

		# Because we may have mixed types of containers,
		# especially during evolution, we cannot
		# statically decide which access method to use (e.g.,
		# based on self.containerType)
		def _put_in_container( c, i, d, orig ):
			if isinstance( c, collections.Mapping ):
				c[i] = d
			else:
				c.append( d )
				if self.set_ids:
					try:
						setattr( orig, StandardInternalFields.ID, len(c) - 1 )
					except AttributeError:
						logger.debug( "Failed to set id", exc_info=True )
		def _get_in_container( c, i, d=None ):
			if isinstance( c, collections.Mapping ):
				return c.get( i, d )
			try:
				return c[i]
			except IndexError:
				return d
		def _remove_in_container( c, d ):
			if isinstance( c, collections.Mapping ):
				for k, v in c.iteritems():
					if v == d:
						del c[k]
						return v
				raise ValueError
			ix = c.index( d )
			d = c[ix]
			c.pop( ix )


		self._v_putInContainer = _put_in_container
		self._v_getInContainer = _get_in_container
		self._v_removeFromContainer = _remove_in_container

	def _v_wrap(self,obj): pass
	def _v_unwrap(self,obj): pass
	def _v_create(self,obj): pass
	def _v_putInContainer( self, obj, orig ): pass
	def _v_getInContainer( self, obj, defv=None ): pass
	def _v_removeFromContainer( self, o ):
		"""
		Raises ValueError if the object is not in the container"
		"""

	def __setstate__( self, dic ):
		super(ContainedStorage,self).__setstate__(dic)
		if not hasattr( self, 'set_ids' ):
			self.set_ids = True
		self._setup()

	def __setattr__( self, name, value ):
		changed = self._p_changed
		super(ContainedStorage,self).__setattr__( name, value )
		# Our volatile attributes should not upset our changed state!
		# Unfortunately, we really have to force this.
		if not changed and (name.startswith( '_v_') or name in ('afterAddContainedObject',
															   'afterGetContainedObject',
															   'afterDeleteContainedObject')):
			self._p_changed = False
			if self._p_jar \
				and self in getattr( self._p_jar, '_registered_objects', ()) \
				and self not in getattr( self._p_jar, '_added', () ):
				getattr( self._p_jar, '_registered_objects' ).remove( self )

	def addContainer( self, containerId, container ):
		"""
		Adds a container using the given containerId, if one does not already
		exist.
		:raises: ValueError If a container already exists.
		:raises: TypeError If container or id is None.
		"""
		if containerId in self.containers:
			raise ValueError( '%s already exists' %(containerId) )
		if container is None or containerId is None:
			raise TypeError( 'Container/Id cannot be None' )
		self.containers[containerId] = container

	def deleteContainer( self, containerId ):
		"""
		Removes an existing container, if one already exists.
		:raises: KeyError If no container exists.
		"""
		del self.containers[containerId]

	def maybeCreateContainedObjectWithType( self, datatype, externalValue ):
		""" If we recognize and own the given datatype, creates
		a new default instance and returns it. Otherwise returns
		None. """
		result = None
		container = self.containers.get( datatype )
		if IHomogeneousTypeContainer.providedBy( container ):
			factory = container.contained_type.queryTaggedValue( IHTC_NEW_FACTORY )
			if factory:
				result = factory( externalValue )
		return result

	def addContainedObject( self, contained ):
		"""
		Given a new object, inserts it in the appropriate container.

		This object should not be contained by anything else
		and should not yet have been persisted. When this method returns,
		the contained object will have an ID (if it already has an ID it
		will be preserved, so long as that doesn't conflict with another object)
		---depending, of course, on the value given at construction time.
		"""


		if not nti_interfaces.IContained.providedBy( contained ):
			raise ValueError( "Contained object (%s) is not IContained" % type(contained) )
		if not getattr( contained, 'containerId' ):
			raise ValueError( "Contained object (%s) has no containerId" % type(contained) )

		container = self.containers.get( contained.containerId, None )
		if container is None:
			container = self.containerType()
			self.containers[contained.containerId] = container

		if isinstance( container, collections.Mapping ):
			# don't allaw adding a new object on top of an existing one,
			# unless the existing one is broken (migration botched, etc)
			if hasattr(contained, StandardInternalFields.ID ) \
				and getattr(contained, StandardInternalFields.ID) \
				and container.get(contained.id,contained) is not contained \
				and ZODB.interfaces.IBroken not in interface.providedBy( container.get( contained.id ) ):
				raise KeyError( "Contained object uses existing ID " + str(contained.id) )

		## Save
		if not contained.id and not self.set_ids:
			raise ValueError( "Contained object has no id and we cannot give it one" )

		# Add to the connection so it can start creating an OID
		# if we are saved, and it is Persistent but unsaved
		if getattr( self, '_p_jar', None ) \
			and getattr( contained, '_p_jar', self ) is None:
			getattr( self, '_p_jar' ).add( contained )

		self._v_create( contained )
		if not contained.id:
			# TODO: Need to allow individual content types some control
			# over this, specifically quizzes. This is a hack for them,
			# which doesn't quite work: they can only generate a good container_key if
			# they already have an ID, and so we don't take this code path
			the_id = None
			if getattr( contained, 'to_container_key', None ):
				the_id = contained.to_container_key()
				if isinstance( container, collections.Mapping ) and container.get( the_id, contained ) is not contained:
					# Don't allow overrwriting
					the_id = None
			if the_id is None:
				the_id = to_external_ntiid_oid( contained )
			contained.id = the_id

		self._v_putInContainer( container,
								getattr(contained, StandardInternalFields.ID, None),
								self._v_wrap( contained ),
								contained )
		# Synchronize the timestamps
		self._updateContainerLM( container )

		self.afterAddContainedObject( contained )

		return contained

	def _updateContainerLM( self, container ):
		self.updateLastMod( )
		up = getattr( container, 'updateLastMod', None )
		if callable( up ):
			up( self.lastModified )

	@property
	def afterAddContainedObject( self ):
		if hasattr( self, '_v_afterAdd' ):
			# We have a default value for this, but it
			# vanishes when we're persisted
			return self._v_afterAdd
		return _noop

	@afterAddContainedObject.setter
	def afterAddContainedObject( self, o ):
		self._v_afterAdd = o

	def deleteContainedObject( self, containerId, containedId ):
		"""
		Given the ID of a container and something contained within it,
		removes that object from the container and returns it. Returns None
		if there is no such object.
		"""
		# In order to share the maximum amount of code, we are first
		# looking the object up and then removing it by equality.
		# NOTE: The reverse DOES NOT work. We may not find the right
		# objects by containedId (if our containers are not maps but lists,
		# and we are just holding shared objects we do not own)
		return self.deleteEqualContainedObject( self.getContainedObject( containerId, containedId ) )


	def deleteEqualContainedObject( self, contained ):
		"""
		Given an object contained herein, removes it. Returns the removed
			object, if found, else returns None.
		"""
		if contained is None or contained.containerId is None:
			logger.debug( "Unable to delete object equal to None or with no containerId: %s", contained )
			return
		container = self.containers.get( contained.containerId, None )
		if container is None:
			logger.debug( "Unable to delete object we have no container for: %s (%s)", contained.containerId, contained )
			return None

		wrapped = self._v_wrap( contained ) # outside the catch
		try:
			contained = self._v_unwrap( self._v_removeFromContainer( container, wrapped ) )
		except ValueError:
			logger.debug( "Failed to find object to delete equal to %s", contained )
			return None
		except TypeError:
			# Getting here means that we are no longer able to resolve
			# at least one object by OID. Might as well take this opportunity
			# to clear out all the dangling refs. Notice we keep the identical
			# container object though
			# FIXME: This code only works when we're using list containers.
			cid = getattr( contained, '_p_oid', self ) or self
			tmp = list( container )
			del container[:]
			for weak in tmp:
				if cid == getattr( weak, 'oid', None ) or \
				   cid == getattr( weak, '_p_oid', None ):
					continue
				strong = weak if not callable( weak ) else weak()
				if strong is not None and strong != contained:
					container.append( strong )
				else:
					logger.debug( "Dropping obj by equality/missing during delete %s == %s", strong, contained )
			return None
		else:
			self._updateContainerLM( container )
			self.afterDeleteContainedObject( contained )
			return contained



	@property
	def afterDeleteContainedObject( self ):
		if hasattr( self, '_v_afterDel' ):
			return self._v_afterDel
		return _noop

	@afterDeleteContainedObject.setter
	def afterDeleteContainedObject( self, o ):
		self._v_afterDel = o

	def getContainedObject( self, containerId, containedId, defaultValue=None ):
		""" Given a container ID and an id within that container,
		retreives the designated object, or the default value (None if not
		specified) if the object cannot be found."""
		container = self.containers.get( containerId )
		if container is None:
			# our unbound method in the other branch
			# means we cannot cheaply use a default value to the
			# get call.
			result = defaultValue
		else:
			result = self._v_getInContainer( container, containedId, defaultValue )
		if result is not defaultValue:
			result = self._v_unwrap( result )
			self.afterGetContainedObject( result )
		return result

	@property
	def afterGetContainedObject( self ):
		if hasattr( self, '_v_afterGet' ):
			return self._v_afterGet
		return _noop

	@afterGetContainedObject.setter
	def afterGetContainedObject( self, o ):
		self._v_afterGet = o

	def getContainer( self, containerId, defaultValue=None ):
		""" Given a container ID, returns the existing container, or
		the default value if there is no container. The returned
		value SHOULD NOT be modified. """
		# FIXME: handle unwrapping.
		return self.containers.get( containerId, defaultValue )

	def __iter__(self):
		return iter(self.containers)

	def __contains__(self,val):
		return val in self.containers

	def __getitem__( self, i ):
		return self.containers[i]

	def iteritems(self):
		return self.containers.iteritems()

class AbstractNamedContainerMap(ModDateTrackingBTreeContainer):
	"""
	A container that implements the basics of a :class:`INamedContainer` as
	a mapping.

	You must supply the `contained_type` attribute and the `container_name`
	attribute.
	"""

	interface.implements( nti_interfaces.IHomogeneousTypeContainer,
						  nti_interfaces.INamedContainer,
						  nti_interfaces.ILastModified )

	contained_type = None
	container_name = None

	def __init__( self, *args, **kwargs ):
		super(AbstractNamedContainerMap,self).__init__( *args, **kwargs )

	def __setitem__(self, key, item):
		# TODO: Finish porting this all over to the constraints in zope.container.
		# That will require specific subtypes for each contained_type (which we already have)
		if not self.contained_type.providedBy( item ):
			raise ValueError( "Item %s for key %s must be %s" % (item,key,self.contained_type) )
		super(AbstractNamedContainerMap,self).__setitem__(key, item)
