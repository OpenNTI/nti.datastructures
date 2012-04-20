#!/usr/bin/env python
"""
Datatypes and datatype handling.
$Revision$
"""


import logging
logger = logging.getLogger( __name__ )

import time
import collections
import UserList
import weakref

import persistent
import BTrees.OOBTree
import ZODB

import six


from zope import interface
from zope import component
from zope.deprecation import deprecated

from .interfaces import (IHomogeneousTypeContainer, IHTC_NEW_FACTORY,
						 						 ILink,	 ILocation)
from . import links

from . import mimetype
from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver import authorization_acl as nacl

import nti.externalization.interfaces as ext_interfaces

# Re-exported
from nti.externalization.oids import fromExternalOID
from nti.externalization.oids import to_external_ntiid_oid
from nti.externalization.oids import toExternalOID
from nti.externalization.externalization import to_json_representation
from nti.externalization.externalization import toExternalDictionary
from nti.externalization.externalization import isSyntheticKey
from nti.externalization.externalization import to_external_representation
from nti.externalization.externalization import toExternalObject
from nti.externalization.externalization import stripSyntheticKeysFromExternalDictionary
from nti.externalization.externalization import DefaultNonExternalizableReplacer
from nti.externalization.externalization import stripNoneFromExternal
from nti.externalization.datastructures import LocatedExternalList
from nti.externalization.datastructures import ExternalizableDictionaryMixin
from nti.externalization.datastructures import LocatedExternalDict
from nti.externalization.datastructures import ExternalizableInstanceDict
from nti.externalization.datastructures import isSyntheticKey
from nti.externalization.persistence import getPersistentState
from nti.externalization.persistence import PersistentExternalizableWeakList
from nti.externalization.persistence import PersistentExternalizableList

if False:
	deprecated( "fromExternalOID", "Prefer nti.externalization.oids.fromExternalOID" )
	deprecated( "to_external_ntiid_oid", "Prefer nti.externalization.oids.to_external_ntiid_oid" )
	deprecated( "toExternalOID", "Prefer nti.externalization.oids.toExternalOID" )
	deprecated( "to_json_representation", "Prefer nti.externalization.externalization.to_json_representation" )
	deprecated( "toExternalDictionary", "Prefer nti.externalization.externalization.toExternalDictionary" )
	deprecated( "isSyntheticKey", "Prefer nti.externalization.externalization.isSyntheticKey" )
	deprecated( "to_external_representation", "Prefer nti.externalization.externalization.to_external_representation" )
	deprecated( "toExternalObject", "Prefer nti.externalization.externalization.toExternalObject" )
	deprecated( "stripSyntheticKeysFromExternalDictionary", "Prefer nti.externalization.externalization.stripSyntheticKeysFromExternalDictionary" )
	deprecated( "DefaultNonExternalizableReplacer", "Prefer nti.externalization.externalization.DefaultNonExternalizableReplacer" )
	deprecated( "stripNoneFromExternal", "Prefer nti.externalization.externalization.stripNoneFromExternal" )
	deprecated( "LocatedExternalList", "Prefer nti.externalization.datastructures.LocatedExternalList" )
	deprecated( "ExternalizableDictionaryMixin", "Prefer nti.externalization.datastructures.ExternalizableDictionaryMixin" )
	deprecated( "LocatedExternalDict", "Prefer nti.externalization.datastructures.LocatedExternalDict" )
	deprecated( "ExternalizableInstanceDict", "Prefer nti.externalization.datastructures.ExternalizableInstanceDict" )
	deprecated( "isSyntheticKey", "Prefer nti.externalization.datastructures.isSyntheticKey" )
	deprecated( "PersistentExternalizableDictionary", "Prefer nti.externalization.persistence.PersistentExternalizableDictionary" )
	deprecated( "getPersistentState", "Prefer nti.externalization.persistence.getPersistentState" )
	deprecated( "setPersistentStateChanged", "Prefer nti.externalization.persistence.setPersistentStateChanged" )
	deprecated( "PersistentExternalizableWeakList", "Prefer nti.externalization.persistence.PersistentExternalizableWeakList" )
	deprecated( "PersistentExternalizableList", "Prefer nti.externalization.persistence.PersistentExternalizableList" )


from nti.externalization.externalization import EXT_FORMAT_JSON, EXT_FORMAT_PLIST


from nti.zodb.minmax import NumericMaximum as _SafeMaximum



class ModDateTrackingObject(object):
	""" Maintains an lastModified attribute containing a time.time()
	modification stamp. Use updateLastMod() to update this value. """


	def __init__( self, *args, **kwargs ):
		# NOTE: In the past, this was a simple number. That lead to dangerous
		# conflict resolution practices, so now it's Maximum. But we don't
		# change this in setstate, we wait until we write the modified value,
		# to avoid writing unnecessary values. Note also that finding these
		# objects and doing a migration is not feasible as they are /everywhere/
		# Some subclasses may depend on being able to update last mod
		# during construction, notably dictionaries initialized as a copy
		self._init_modified()
		super(ModDateTrackingObject,self).__init__( *args, **kwargs )

	def _init_modified(self):
		self._lastModified = _SafeMaximum(value=0)

	def _get_lastModified(self):
		# To make it easy to add this class as a mixin
		# to any class, some of which may already be in the
		# database, we handle missing last modified values
		try:
			return self._lastModified.value
		except AttributeError:
			try:
				return self._lastModified
			except AttributeError:
				return 0
	def _set_lastModified(self, lm):
		old_lm = getattr( self, '_lastModified', None )
		if not hasattr( old_lm, 'value' ):
			self._lastModified = _SafeMaximum( value=lm )
		else:
			self._lastModified.value = lm
	lastModified = property( _get_lastModified, _set_lastModified )

	def updateLastMod(self, t=None ):
		self.lastModified = t if t is not None and t > self.lastModified else time.time()
		return self.lastModified

	def updateLastModIfGreater( self, t ):
		"Only if the given time is (not None and) greater than this object's is this object's time changed."
		if t is not None and t > self.lastModified:
			self.lastModified = t
		return self.lastModified

def _syntheticKeys( ):
	return ('OID', 'ID', 'Last Modified', 'Creator', 'ContainerId', 'Class')

def _isMagicKey( key ):
	""" For our mixin objects that have special keys, defines
	those keys that are special and not settable by the user. """
	return key in _syntheticKeys()

isSyntheticKey = _isMagicKey

from nti.externalization.interfaces import StandardInternalFields, StandardExternalFields


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


class LinkDecorator(object):
	interface.implements(ext_interfaces.IExternalMappingDecorator)
	component.adapts(object)

	def __init__( self, o ):
		pass

	def decorateExternalMapping( self, orig, result ):
		_links = find_links(orig)
		_links = [toExternalObject(l) for l in _links if l]
		_links = [l for l in _links if l]
		if _links:
			for link in _links:
				interface.alsoProvides( link, ILocation )
				link.__name__ = ''
				link.__parent__ = self
			result[StandardExternalFields.LINKS] = _links

class ACLDecorator(object):
	interface.implements(ext_interfaces.IExternalMappingDecorator)
	component.adapts(object)

	def __init__( self, o ):
		pass

	def decorateExternalMapping( self, orig, result ):
		result.__acl__ = nacl.ACL( orig )

class MimeTypeDecorator(object):
	interface.implements(ext_interfaces.IExternalMappingDecorator)
	component.adapts(object)

	def __init__( self, o ):
		pass

	def decorateExternalMapping( self, orig, result ):
		if StandardExternalFields.CLASS in result and StandardExternalFields.MIMETYPE not in result:
			mime_type = mimetype.nti_mimetype_from_object( orig, use_class=False )
			if mime_type:
				result[StandardExternalFields.MIMETYPE] = mime_type

class LinkNonExternalizableReplacer(object):
	"We expect higher levels to handle links, so we let them through."
	# TODO: This probably belongs /at/ that higher level, not here
	interface.implements(ext_interfaces.INonExternalizableReplacer)
	component.adapts(ILink)

	def __init__( self, o ):
		pass

	def __call__( self, link ):
		return link


class CreatedModDateTrackingObject(ModDateTrackingObject):
	""" Adds the `creator` and `createdTime` attributes. """
	def __init__( self, *args ):
		super(CreatedModDateTrackingObject,self).__init__( *args )
		# Some of our subclasses have class attributes for fixed creators.
		# don't override those unless we have to
		if not hasattr(self, 'creator'):
			self.creator = None
		self.createdTime = time.time()

class PersistentCreatedModDateTrackingObject(persistent.Persistent,CreatedModDateTrackingObject):
	pass

class ModDateTrackingMappingMixin(CreatedModDateTrackingObject):

	def __init__( self, *args ):
		super(ModDateTrackingMappingMixin, self).__init__( *args )

	def updateLastMod(self, t=None ):
		ModDateTrackingObject.updateLastMod( self, t )
		# FIXME: This produces artificial conflicts.
		# Convert this and the superclass to use zope.minmax.Maximum
		# Will require changes to some iteration places to avoid that value
		# We really just don't want this value in the map
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
	# This class and subclasses
	# do not preserve custom attributes like 'lastModified'
	# due to the implementation of __getstate__ in OOBTree.
	# Thus, we lose them across persistence. This is hidden by
	# defaults in another superclass. Some things compensate by checking
	# the 'Last Modified' entry, but that's not good to have either.
	# Therefore, we override all of the methods from the superclass
	# in terms of the dictionary entry added my ModDateTrackingMappingMixin.
	# We want to avoid any extra objects that might get added to the connection,
	# but never be accessed again, so we avoid the _lastModified attribute
	# entirely (hence picking the implementations instead of super())
	def __init__(self, *args):
		BTrees.OOBTree.OOBTree.__init__( self, *args )

	def _init_modified(self):
		return

	def toExternalDictionary(self, mergeFrom=None):
		result = super(ModDateTrackingOOBTree,self).toExternalDictionary(mergeFrom)
		for key, value in self.iteritems():
			result[key] = toExternalObject( value )
		return result

	def updateLastMod(self, t=None):
		super(ModDateTrackingMappingMixin,self).__setitem__(StandardExternalFields.LAST_MODIFIED,
															t if t is not None and t > self.lastModified else time.time() )
		return self.lastModified

	def _get_lastModified( self ):
		return self.get( StandardExternalFields.LAST_MODIFIED, 0 )
	def _set_lastModified( self, lm ):
		BTrees.OOBTree.OOBTree.__setitem__( self, StandardExternalFields.LAST_MODIFIED, lm )
	lastModified = property(_get_lastModified,_set_lastModified)

	def _p_resolveConflict(self, oldState, savedState, newState ):
		# Our super class will generally resolve what conflicts it
		# can, or throw an exception. If it resolves things,
		# we just want to update our last modified time---that's the thing
		# most likely to conflict

		# A BTree writes its state as a sequence of tuples, for each bucket.
		# A bucket may be an OOBucket, or a tuple itself, if it is small.
		# If we're small enough that the conflict is in this object, then
		# it must be a tuple
		logger.info( 'Conflict to resolve in %s', type(self) )
		# We just make the last modified to be now
		lm = time.time()
		def maxing( state ):
			if isinstance( state, tuple ):
				state = list(state)
				i = 0
				while i < len(state):
					eq = False
					try:
						eq = (state[i] == StandardExternalFields.LAST_MODIFIED)
					except ValueError:
						# Probably a PersistentReference object we're not interested in
						eq = False

					if eq:
						state[i+1] = lm
						i += 1
					else:
						state[i] = maxing(state[i])
					i += 1
				state = tuple(state)
			return state

		# Note that we're not using super(), we're actually picking the implementation
		# The one we get from ModDateTrackingMappingMixin fails with this structure
		result = BTrees.OOBTree.OOBTree._p_resolveConflict( self,
															maxing( oldState ),
															maxing( savedState ),
															maxing( newState ) )
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

PersistentExternalizableList.__bases__ = (ModDateTrackingObject,persistent.list.PersistentList)
_PersistentExternalizableWeakList = PersistentExternalizableWeakList

class PersistentExternalizableWeakList(_PersistentExternalizableWeakList,CreatedModDateTrackingObject):
	"""
	Stores :class:`persistent.Persistent` objects as weak references, invisibly to the user.
	Any weak references added to the list will be treated the same.
	"""

	def remove(self,value):
		super(PersistentExternalizableWeakList,self).remove( value )
		self.updateLastMod()

	def __setitem__(self, i, item):
		super(PersistentExternalizableWeakList,self).__setitem__( i, item )
		self.updateLastMod()

	def __iadd__(self, other):
		# We must wrap each element in a weak ref
		# Note that the builtin list only accepts other lists,
		# but the UserList from which we are descended accepts
		# any iterable.
		result = super(PersistentExternalizableWeakList,self).__iadd__(other)
		self.updateLastMod()
		return result

	def __imul__(self, n):
		result = super(PersistentExternalizableWeakList,self).__imul__(n)
		self.updateLastMod()
		return result

	def append(self, item):
		super(PersistentExternalizableWeakList,self).append(item)
		self.updateLastMod()

	def insert(self, i, item):
		super(PersistentExternalizableWeakList,self).insert( i, item )
		self.updateLastMod()

	def pop(self, i=-1):
		rtn = super(PersistentExternalizableWeakList,self).pop( i )
		self.updateLastMod()
		return rtn


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

class _ContainedObjectValueError(ValueError):
	"""
	A more naturally descriptive exception for contained objects.
	"""
	def __init__( self, string, contained=None ):
		ctype = type(contained)
		cstr = 'Unable to determine'
		try:
			cstr = repr(contained)
		except Exception as e:
			cstr = '{%s}' % e
		super(_ContainedObjectValueError,self).__init__( "%s [type: %s repr %s]" % (string, ctype, cstr) )

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
			raise _ContainedObjectValueError( "Contained object is not IContained", contained )
		if not getattr( contained, 'containerId' ):
			raise _ContainedObjectValueError( "Contained object has no containerId", contained )

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
			raise _ContainedObjectValueError( "Contained object has no id and we are not allowed to give it one.", contained )

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
			return None
		container = self.containers.get( contained.containerId )
		if container is None:
			logger.debug( "Unable to delete object we have no container for: %s (%s) (%s) (%s %r %r %r)",
								  contained.containerId, list(self.containers.keys()),
								  self.containers._p_state, self.containers._p_jar, self.containers._p_oid, self.containers._p_serial,
								  contained )
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

from nti.zodb.minmax import MergingCounter
deprecated( "MergingCounter", "Prefer nti.zodb.minmax" )
