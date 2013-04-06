#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Datatypes and datatype handling.

$Id$
"""

from __future__ import print_function, unicode_literals, absolute_import
__docformat__ = "restructuredtext en"

import logging
logger = logging.getLogger(__name__)

import time
import datetime
import collections
import numbers
import weakref

import persistent
import BTrees.OOBTree
import ZODB


from zope import interface
from zope import component
from zope.deprecation import deprecated
from zope.container import contained as zcontained, btree
from zope.location import interfaces as loc_interfaces
from zope.dublincore import interfaces as dc_interfaces

from .interfaces import (IHomogeneousTypeContainer, IHTC_NEW_FACTORY,
                         ILink)
from . import links

from . import mimetype
from nti.dataserver import interfaces as nti_interfaces
from nti.dataserver import containers as container
import nti.externalization.interfaces as ext_interfaces

# Deprecated below, used in this module. Re-exported for b/c
from nti.externalization.oids import to_external_ntiid_oid
from nti.externalization.externalization import toExternalObject
from nti.externalization.datastructures import ExternalizableDictionaryMixin, LocatedExternalList, LocatedExternalDict
from nti.externalization.persistence import PersistentExternalizableWeakList
from nti.externalization.persistence import PersistentExternalizableList
from nti.externalization.singleton import SingletonDecorator

from nti.zodb import minmax
from nti.zodb.persistentproperty import PersistentPropertyHolder

class ModDateTrackingObject(object):
	"""
	Maintains an lastModified attribute containing a time.time()
	modification stamp. Use updateLastMod() to update this value.
	Typically subclasses of this class should be :class:`nti.zodb.persistentproperty.PersistentPropertyHolder`
	"""

	lastModified = minmax.NumericPropertyDefaultingToZero( '_lastModified', minmax.NumericMaximum, as_number=True )

	def __new__( cls, *args, **kwargs ):
		if issubclass(cls, persistent.Persistent) and not issubclass(cls, PersistentPropertyHolder):
			print("ERROR: subclassing Persistent, but not PersistentPropertyHolder", cls)
		return super(ModDateTrackingObject,cls).__new__( cls, *args, **kwargs )

	def __init__( self, *args, **kwargs ):
		super(ModDateTrackingObject,self).__init__( *args, **kwargs )

	def __setstate__( self, state ):
		if '_lastModified' in state and isinstance( state['_lastModified'], numbers.Number ):
			# Are there actually any objects still around that have this condition?
			# A migration to find them is probably difficult
			state['_lastModified'] = minmax.NumericMaximum(state['_lastModified'])
		# We may or may not be the base of the inheritance tree; usually we are not,
		# but occasionally (mostly in tests) we are
		try:
			super(ModDateTrackingObject,self).__setstate__(state)
		except AttributeError:
			self.__dict__.clear()
			self.__dict__.update( state )

	def updateLastMod(self, t=None ):
		self.lastModified = ( t if t is not None and t > self.lastModified else time.time() )
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

# For speed and use in this function, we declare an 'inline'-able attribute

_magic_keys = set( _syntheticKeys() )

from nti.externalization.interfaces import StandardInternalFields, StandardExternalFields
deprecated( "StandardExternalFields", "Prefer nti.externalization.interfaces" )
deprecated( "StandardInternalFields", "Prefer nti.externalization.interfaces" )


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

@interface.implementer(ext_interfaces.IExternalMappingDecorator)
@component.adapter(object)
class LinkDecorator(object):

	__metaclass__ = SingletonDecorator

	def decorateExternalMapping( self, context, result ):
		# We have no way to know what order these will be
		# called in, so we must preserve anything that exists
		orig_links = result.get( StandardExternalFields.LINKS, () )
		_links = find_links(context)
		_links = [toExternalObject(l) for l in _links if l]
		_links = [l for l in _links if l] # strip none
		if _links:
			_links = sorted(_links)
			for link in _links:
				interface.alsoProvides( link, loc_interfaces.ILocation )
				link.__name__ = ''
				link.__parent__ = context

		_links.extend( orig_links )
		if _links:
			result[StandardExternalFields.LINKS] = _links


@interface.implementer(ext_interfaces.INonExternalizableReplacer)
@component.adapter(ILink)
class LinkNonExternalizableReplacer(object):
	"We expect higher levels to handle links, so we let them through."
	# TODO: This probably belongs /at/ that higher level, not here

	def __init__( self, o ):
		pass

	def __call__( self, link ):
		return link

@interface.implementer(dc_interfaces.IDCTimes)
class CreatedModDateTrackingObject(ModDateTrackingObject):
	""" Adds the `creator` and `createdTime` attributes. """
	def __init__( self, *args, **kwargs ):
		self.createdTime = time.time()
		self.updateLastModIfGreater( self.createdTime )

		super(CreatedModDateTrackingObject,self).__init__( *args, **kwargs )

		# Some of our subclasses have class attributes for fixed creators.
		# don't override those unless we have to
		if not hasattr(self, 'creator'):
			self.creator = None

	created = property( lambda self: datetime.datetime.fromtimestamp( self.createdTime ),
						lambda self, dt: setattr( self, 'createdTime', time.mktime( dt.timetuple() ) ) )
	modified = property( lambda self: datetime.datetime.fromtimestamp( self.lastModified ),
						lambda self, dt: self.updateLastModIfGreater( time.mktime( dt.timetuple() ) ) )



class PersistentCreatedModDateTrackingObject(CreatedModDateTrackingObject,PersistentPropertyHolder):
	# order of inheritance matters; if Persistent is first, we can't have our own __setstate__;
	# only subclasses can
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
		if  key in _magic_keys:
			return
		super(ModDateTrackingMappingMixin, self).__delitem__(key)
		self.updateLastMod()

	def __setitem__(self, key, y):
		if key in _magic_keys:
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



class ModDateTrackingOOBTree(PersistentPropertyHolder,ModDateTrackingMappingMixin, BTrees.OOBTree.OOBTree, ExternalizableDictionaryMixin):
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
		BTrees.OOBTree.OOBTree.__setitem__(self, StandardExternalFields.LAST_MODIFIED,
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
		# most likely to conflict.

		# Note that a conflict it cannot resolve is if both savedState and newState
		# get the addition of the same new key. (e.g., two transactions both add
		# the same new key). Given some application knowledge, we might
		# be able to merge the values for those keys.

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
	"""
	This class implements a dictionary key that preserves case, but
	compares case-insensitively.

	This is a bit of a heavyweight solution. It is nonetheless optimized for comparisons
	only with other objects of its same type. It must not be subclassed.
	"""

	def __init__( self, key ):
		self.key = key
		self._lower_key = key.lower()

	def __str__( self ):
		return self.key

	def __repr__( self ):
		return "%s('%s')" % (self.__class__, self.key)

	def __eq__(self, other):
		try:
			return other is self or other._lower_key == self._lower_key
		except AttributeError:
			return NotImplemented

	def __hash__(self):
		return hash(self._lower_key)

	### NOTE: This class is slightly broken for ordering comparisons.
	# We allow comparing ourself to string (and only strings)
	# instead of return NotImplemented. This is not right, because
	# equality doesn't do this. But it is necessary for backwards
	# compatibility with existing sorted BTrees.
	# TODO: How to really fix this?
	# TODO: Could this lead to data loss? Something not less than, not greater
	# than, but also not equal to something else?

	def __lt__(self, other):
		try:
			return self._lower_key < other._lower_key
		except AttributeError:
			return self._lower_key < other

	def __gt__(self, other):
		try:
			return self._lower_key > other._lower_key
		except AttributeError:
			return self._lower_key > other

from repoze.lru import lru_cache

# These work best as plain functions so that the 'self'
# argument is not captured. The self argument is persistent
# and so that messes with caches
@lru_cache(10000)
def _tx_key_lower(key):
	# updateLastModified doesn't go through our transformation,
	# so we must also not transform the Last Modified key
	if isinstance( key, basestring ) and key not in _magic_keys: # use basestring, not six.stringtypes, marginally faster
		key = key.lower()
	return key

@lru_cache(10000)
def _tx_key_insen(key):
	if isinstance( key, basestring ) and key not in _magic_keys: # use basestring, not six.stringtypes, marginally faster
		key = _CaseInsensitiveKey( key )
	return key

class CaseInsensitiveModDateTrackingOOBTree(ModDateTrackingOOBTree):
	"""
	This class should not be used as it changes the stored keys.
	"""
	# This is left for backwards compatibility.

	def __init__(self, *args ):
		super(CaseInsensitiveModDateTrackingOOBTree, self).__init__( *args )

	def _tx_key( self, key ):
		return _tx_key_lower( key )

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
		return _tx_key_insen( key )

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
# See the notes in that package. It's not safe to subclass btrees.
# Consider zc.dict if necessary
deprecated( 'KeyPreservingCaseInsensitiveModDateTrackingOOBTree', 'Use nti.dataserver.container instead' )
deprecated( 'CaseInsensitiveModDateTrackingOOBTree', 'Use nti.dataserver.container instead' )
deprecated( 'ModDateTrackingOOBTree', 'Use nti.dataserver.container instead' )
deprecated( 'ModDateTrackingMappingMixin', 'Stores a key in the dictionary, not recommended.' )

class ModDateTrackingPersistentMapping(PersistentPropertyHolder, ModDateTrackingMappingMixin, persistent.mapping.PersistentMapping, ExternalizableDictionaryMixin):

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


class LastModifiedCopyingUserList(ModDateTrackingObject,list):
	""" For building up a sequence of lists, keeps the max last modified. """
	def extend( self, other ):
		super(LastModifiedCopyingUserList,self).extend( other )
		self.updateLastModIfGreater( getattr( other, 'lastModified', self.lastModified ) )

	def __iadd__( self, other ):
		result = super(LastModifiedCopyingUserList,self).__iadd__( other )
		self.updateLastModIfGreater( getattr( other, 'lastModified', self.lastModified ) )
		return result

	def __reduce__( self ):
		raise TypeError("Transient object.")

from persistent.wref import WeakRef
# WTF we doing here?
PersistentExternalizableList.__bases__ = (PersistentPropertyHolder,ModDateTrackingObject,persistent.list.PersistentList)
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

from nti.utils.schema import UnicodeConvertingFieldProperty

@interface.implementer(nti_interfaces.IContained)
class _ContainedMixin(zcontained.Contained):
	"""
	Defines something that can be logically contained inside another unit
	by reference. Two properties are defined, id and containerId.
	"""

	# It is safe to use these properties in persistent objects because
	# they read/write to the __dict__ with the same name as the field,
	# and setattr on the persistent object is what set _p_changed, so
	# assigning to them still changes the object correctly
	containerId = UnicodeConvertingFieldProperty(nti_interfaces.IContained['containerId'])
	id = UnicodeConvertingFieldProperty(nti_interfaces.IContained['id'])

	# __name__ is NOT automatically defined as an id alias, because that could lose
	# access to existing data that has a __name__ in its instance dict

	def __init__(self, *args, **kwargs ):
		containerId = kwargs.pop( 'containerId', None )
		containedId = kwargs.pop( 'containedId', None )
		super(_ContainedMixin,self).__init__(*args, **kwargs)
		if containerId is not None:
			self.containerId = containerId
		if containedId is not None:
			self.id = containedId

ContainedMixin = ZContainedMixin = _ContainedMixin

@interface.implementer( nti_interfaces.ILastModified )
class ModDateTrackingBTreeContainer(btree.BTreeContainer):

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

deprecated( 'ModDateTrackingBTreeContainer', "use nti.dataserver.container classes instead." )
deprecated( 'KeyPreservingCaseInsensitiveModDateTrackingBTreeContainer', "use nti.dataserver.container classes instead." )

def _noop(*args): pass

class _ContainedObjectValueError(ValueError):
	"""
	A more naturally descriptive exception for contained objects.
	"""
	def __init__( self, string, contained=None, **kwargs ):
		ctype = type(contained)
		cstr = 'Unable to determine'
		try:
			cstr = repr(contained)
		except Exception as e:
			cstr = '{%s}' % e
		super(_ContainedObjectValueError,self).__init__( "%s [type: %s repr %s]%s" % (string, ctype, cstr, kwargs) )

def check_contained_object_for_storage( contained ):
	if not nti_interfaces.IContained.providedBy( contained ):
		raise _ContainedObjectValueError( "Contained object is not " + str(nti_interfaces.IContained), contained )
	if not nti_interfaces.IZContained.providedBy( contained ):
		raise _ContainedObjectValueError( "Contained object is not " + str(nti_interfaces.IZContained), contained )

	if not getattr( contained, 'containerId' ):
		raise _ContainedObjectValueError( "Contained object has empty containerId", contained )


from zope.location import locate as loc_locate

@interface.implementer(nti_interfaces.IZContained, loc_interfaces.ISublocations)
class ContainedStorage(PersistentPropertyHolder,ModDateTrackingObject):
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

	__parent__ = None
	__name__ = None

	# TODO: Remove the containerType argument; nothing except tests uses it now, everything else uses the standard type.
	# That will let us remove the complicated code to do different things based on the type of container.
	def __init__( self, weak=False, create=False, containers=None, containerType=container.CheckingLastModifiedBTreeContainer,
				  set_ids=True, containersType=BTrees.OOBTree.OOBTree):
		"""
		Creates a new container.

		:param bool weak: If true, we will maintain weak references to contained objects.
		:param object create: A boolean or object value. If it is true, the `creator` property
			of objects added to us will be set. If `create` is a boolean, this `creator` property
			will be set to this object (useful for subclassing). Otherwise, the `creator` property
			will be set to the value of `create`.
		:param dict containers: Initial containers. We do not adopt these containers, they may already have a __parent__
			(presumably an ancestor of ours as well)
		:param type containerType: The type for each created container. Should be a mapping
			type, and should handle conflicts. The default value only allows comparable keys.
			The type can also be a `list` type, though this use is deprecated and discouraged.
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
			# Notice that we're not using addContainer: these don't
			# become our children.
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
						setattr( orig, StandardInternalFields.ID, unicode(len(c) - 1) )
					except AttributeError:
						logger.debug( "Failed to set id", exc_info=True )
		def _get_in_container( c, i, d=None ):
			if isinstance( c, collections.Mapping ):
				return c.get( i, d ) if i is not None else d # BTree containers raise TypeError on a None key
			try:
				return c[int(i)]
			except IndexError:
				return d
		def _remove_in_container( c, d ):
			if isinstance( c, collections.Mapping ):
				for k, v in c.iteritems():
					if v == d:
						del c[k]
						return v
				raise ValueError(d)
			# Lists. Note that duplicates may have
			# crept in. TODO: We should probably remove them all
			ix = None
			ix = c.index( d )
			d = c[ix]
			c.pop( ix )
			return d


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

	def addContainer( self, containerId, container, locate=True ):
		"""
		Adds a container using the given containerId, if one does not already
		exist.

		:keyword bool locate: If ``True`` (the default), then the given container
			object will be located as a named child of this object. (Assuming
			it provides :class:`.ILocation`).

		:raises: ValueError If a container already exists.
		:raises: TypeError If container or id is None.
		"""
		if containerId in self.containers:
			raise ValueError( '%s already exists' %(containerId) )
		if container is None or containerId is None:
			raise TypeError( 'Container/Id cannot be None' )
		self.containers[containerId] = container
		if locate and loc_interfaces.ILocation.providedBy( container ):
			loc_locate( container, self, containerId )

	def deleteContainer( self, containerId ):
		"""
		Removes an existing container, if one already exists.
		:raises: KeyError If no container exists.
		"""
		del self.containers[containerId]

	def getContainer( self, containerId, defaultValue=None ):
		""" Given a container ID, returns the existing container, or
		the default value if there is no container. The returned
		value SHOULD NOT be modified. """
		# FIXME: handle unwrapping of the contained objects
		return self.containers.get( containerId, defaultValue )

	def getOrCreateContainer( self, containerId ):
		"""
		Return a container for the given containerId. If one
		does not already exist, it will be created and stored.
		"""
		container = self.containers.get( containerId, None )
		if container is None:
			container = self.containerType()
			if getattr( self, '_p_jar', None ) and hasattr( container, '_p_jar' ):
				getattr( self, '_p_jar' ).add( container )
			self.addContainer( containerId, container )
		return container

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

		# Notice we don't proxy to these interfaces, as zope.container does
		# (and would do automatically for IZContained). That results in extra objects
		# in the database and some confusing messages. Easier to ensure that all objects
		# meet our requirements
		check_contained_object_for_storage( contained )

		container = self.getOrCreateContainer( contained.containerId )

		if isinstance( container, collections.Mapping ):
			# don't allaw adding a new object on top of an existing one,
			# unless the existing one is broken (migration botched, etc).
			# Be idempotent, though, and ignore the same object (taking wrapping into account)
			if contained.id:
				existing = container.get( contained.id, None )
				if existing is not None:
					existing = self._v_unwrap( existing )
					if existing is contained:
						return existing # Nothing more do do
					# OK, so it's not contained. Is it broken?
					if ZODB.interfaces.IBroken not in interface.providedBy( existing ):
						__traceback_info__ = contained, existing
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
				the_id = to_external_ntiid_oid( contained, add_to_intids=True )
			contained.id = the_id

		__traceback_info__ = container, contained.containerId, contained.id
		if contained.id is None:
			raise _ContainedObjectValueError( "Unable to determine contained id", contained )

		self._v_putInContainer( container,
								contained.id,
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


	def deleteEqualContainedObject( self, contained, log_level=logging.DEBUG ):
		"""
		Given an object contained herein, removes it. Returns the removed
			object, if found, else returns None.

		:param log_level: The level at which we log if we are unable to delete
			the object. If this is expected to be common and harmless, set it lower than DEBUG.
		"""
		if contained is None or contained.containerId is None:
			logger.log( log_level, "Unable to delete object equal to None or with no containerId: %s", contained )
			return None
		container = self.containers.get( contained.containerId )
		if container is None:
			logger.log(
				log_level,
				"Unable to delete object we (%r) have no container for: %s (%s) (%s) (%s %r %r %r)",
				self,
				contained.containerId, list(self.containers.keys()),
				self.containers._p_state, self.containers._p_jar, self.containers._p_oid, self.containers._p_serial,
				contained )
			return None

		wrapped = self._v_wrap( contained ) # outside the catch
		try:
			contained = self._v_unwrap( self._v_removeFromContainer( container, wrapped ) )
		except ValueError:
			logger.log( log_level, "Failed to find object to delete equal to %s", contained )
			return None
		except TypeError:
			logger.log( log_level, "Failed to find object to delete equal to %s", contained, exc_info=True )
			# Getting here means that we are no longer able to resolve
			# at least one object by OID. Might as well take this opportunity
			# to clear out all the dangling refs. Notice we keep the identical
			# container object though
			# FIXME: This code only works when we're using list containers.
			if isinstance( container, collections.Mapping ):
				raise
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
					logger.log( log_level, "Dropping obj by equality/missing during delete %s == %s", strong, contained )
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


	def __iter__(self):
		return iter(self.containers)

	def __contains__(self,val):
		return val in self.containers

	def __getitem__( self, i ):
		return self.containers[i]

	def iteritems(self):
		return self.containers.iteritems()

	def itervalues(self):
		return self.containers.itervalues()

	values = itervalues

	def iter_all_contained_objects(self):
		""" Only works for dict-like containers """
		for container in self.itervalues():
			for v in container.values():
				yield v

	def sublocations(self):
		return (container for container
				in self.itervalues()
				# Recall that we could be holding containers given to __init__ that we are not the parent of
				if (loc_interfaces.ILocation.providedBy(container) and container.__parent__ is self))

	def __repr__( self ):
		return "<%s size: %s name: %s>" % (self.__class__.__name__, len(self.containers), self.__name__)

from zope.container.constraints import checkObject
from zope.container.interfaces import InvalidItemType

@interface.implementer( nti_interfaces.IHomogeneousTypeContainer,
						nti_interfaces.INamedContainer,
						nti_interfaces.ILastModified )
class AbstractNamedContainerMap(ModDateTrackingBTreeContainer):
	"""
	A container that implements the basics of a :class:`INamedContainer` as
	a mapping.

	You must supply the `contained_type` attribute and the `container_name`
	attribute.

	You *should* define a specific interface for each type of homogeneous
	container. This interface should use :func:`zope.container.constraints.contains`
	to declare that it `contains` the `contained_type`. (The `contained_type` will
	one day be deprecated.) This object will check the constraints declared
	in the interfaces for the container and the objects.
	"""

	contained_type = None
	container_name = None

	def __init__( self, *args, **kwargs ):
		super(AbstractNamedContainerMap,self).__init__( *args, **kwargs )

	def __setitem__(self, key, item):
		# TODO: Finish porting this all over to the constraints in zope.container.
		# That will require specific subtypes for each contained_type (which we already have)
		# We start the process by using checkObject to validate any preconditions
		# that are defined
		checkObject( self, key, item )
		if not self.contained_type.providedBy( item ):
			raise InvalidItemType( self, item, (self.contained_type,) )
		super(AbstractNamedContainerMap,self).__setitem__(key, item)

deprecated('AbstractNamedContainerMap', "Prefer AbstractNamedLastModifiedBTreeContainer" )

@interface.implementer( nti_interfaces.IHomogeneousTypeContainer,
						nti_interfaces.INamedContainer,
						nti_interfaces.ILastModified )
class AbstractNamedLastModifiedBTreeContainer(container.LastModifiedBTreeContainer):
	"""
	A container that implements the basics of a :class:`INamedContainer` as
	a mapping.

	You must supply the `contained_type` attribute and the `container_name`
	attribute.

	You *should* define a specific interface for each type of homogeneous
	container. This interface should use :func:`zope.container.constraints.contains`
	to declare that it `contains` the `contained_type`. (The `contained_type` will
	one day be deprecated.) This object will check the constraints declared
	in the interfaces for the container and the objects.
	"""


	contained_type = None
	container_name = None

	def __init__( self, *args, **kwargs ):
		super(AbstractNamedLastModifiedBTreeContainer,self).__init__( *args, **kwargs )

	def __setitem__(self, key, item):
		# TODO: Finish porting this all over to the constraints in zope.container.
		# That will require specific subtypes for each contained_type (which we already have)
		# We start the process by using checkObject to validate any preconditions
		# that are defined
		checkObject( self, key, item )
		if not self.contained_type.providedBy( item ):
			raise InvalidItemType( self, item, (self.contained_type,) )
		super(AbstractNamedLastModifiedBTreeContainer,self).__setitem__(key, item)

class AbstractCaseInsensitiveNamedLastModifiedBTreeContainer(container.CaseInsensitiveLastModifiedBTreeContainer,AbstractNamedLastModifiedBTreeContainer):
	pass

from nti.zodb.minmax import MergingCounter
deprecated( "MergingCounter", "Prefer nti.zodb.minmax" )



# deprecated( "fromExternalOID", "Prefer nti.externalization.oids.fromExternalOID" )
deprecated( "to_external_ntiid_oid", "Prefer nti.externalization.oids.to_external_ntiid_oid" )
# deprecated( "toExternalOID", "Prefer nti.externalization.oids.toExternalOID" )
# deprecated( "to_json_representation", "Prefer nti.externalization.externalization.to_json_representation" )
# deprecated( "toExternalDictionary", "Prefer nti.externalization.externalization.toExternalDictionary" )
# deprecated( "isSyntheticKey", "Prefer nti.externalization.externalization.isSyntheticKey" )
# deprecated( "to_external_representation", "Prefer nti.externalization.externalization.to_external_representation" )
deprecated( "toExternalObject", "Prefer nti.externalization.externalization.toExternalObject" )
# deprecated( "stripSyntheticKeysFromExternalDictionary", "Prefer nti.externalization.externalization.stripSyntheticKeysFromExternalDictionary" )
# deprecated( "DefaultNonExternalizableReplacer", "Prefer nti.externalization.externalization.DefaultNonExternalizableReplacer" )
# deprecated( "stripNoneFromExternal", "Prefer nti.externalization.externalization.stripNoneFromExternal" )
# There may be persistent data referring to these still
deprecated( "LocatedExternalList", "Prefer nti.externalization.datastructures.LocatedExternalList" )
deprecated( "ExternalizableDictionaryMixin", "Prefer nti.externalization.datastructures.ExternalizableDictionaryMixin" )
deprecated( "LocatedExternalDict", "Prefer nti.externalization.datastructures.LocatedExternalDict" )
deprecated( "ExternalizableInstanceDict", "Prefer nti.externalization.datastructures.ExternalizableInstanceDict" )
# deprecated( "isSyntheticKey", "Prefer nti.externalization.datastructures.isSyntheticKey" )
deprecated( "PersistentExternalizableDictionary", "Prefer nti.externalization.persistence.PersistentExternalizableDictionary" )
# deprecated( "getPersistentState", "Prefer nti.externalization.persistence.getPersistentState" )
# deprecated( "setPersistentStateChanged", "Prefer nti.externalization.persistence.setPersistentStateChanged" )
deprecated( "PersistentExternalizableWeakList", "Prefer nti.externalization.persistence.PersistentExternalizableWeakList" )
deprecated( "PersistentExternalizableList", "Prefer nti.externalization.persistence.PersistentExternalizableList" )


# from nti.externalization.externalization import EXT_FORMAT_JSON, EXT_FORMAT_PLIST
