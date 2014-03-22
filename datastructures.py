#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Datatypes and datatype handling.

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
import logging
import numbers
import weakref
import collections

import BTrees.OOBTree

import persistent
from persistent.wref import WeakRef

import ZODB

from zope import interface
from zope import component

import zope.deferredimport
zope.deferredimport.initialize()

from zope.container.constraints import checkObject
from zope.container.interfaces import InvalidItemType
from zope.container import contained as zcontained

from zope.location import locate as loc_locate
from zope.location import interfaces as loc_interfaces

from . import containers as container
from . import interfaces as nti_interfaces

import nti.externalization.interfaces as ext_interfaces

from nti.zodb import minmax
from nti.zodb.persistentproperty import PersistentPropertyHolder
from nti.zodb.persistentproperty import PropertyHoldingPersistent

from . import links
from .interfaces import (IHomogeneousTypeContainer, IHTC_NEW_FACTORY, ILink)


from nti.externalization.oids import to_external_ntiid_oid
from nti.externalization.singleton import SingletonDecorator
from nti.externalization.externalization import toExternalObject
from nti.externalization.persistence import PersistentExternalizableWeakList as _PersistentExternalizableWeakList
from nti.externalization.persistence import PersistentExternalizableList as _PersistentExternalizableList

class ModDateTrackingObject(object):
	"""
	Maintains an lastModified attribute containing a time.time()
	modification stamp. Use updateLastMod() to update this value.
	Typically subclasses of this class should be :class:`nti.zodb.persistentproperty.PersistentPropertyHolder`
	"""

	lastModified = minmax.NumericPropertyDefaultingToZero( str('_lastModified'), minmax.NumericMaximum, as_number=True )

	def __new__( cls, *args, **kwargs ):
		if issubclass(cls, persistent.Persistent) and not issubclass(cls, PersistentPropertyHolder):
			print("ERROR: subclassing Persistent, but not PersistentPropertyHolder", cls)
		return super(ModDateTrackingObject,cls).__new__( cls, *args, **kwargs )

	def __init__( self, *args, **kwargs ):
		super(ModDateTrackingObject,self).__init__( *args, **kwargs )

	def __setstate__(self, data):
		if isinstance(data, collections.Mapping) and '_lastModified' in data and isinstance(data['_lastModified'], numbers.Number):
			# Are there actually any objects still around that have this condition?
			# A migration to find them is probably difficult
			data['_lastModified'] = minmax.NumericMaximum(data['_lastModified'])
		elif isinstance(data, (float, int)):  # Not sure why we get float here
			data = {'_lastModified':minmax.NumericMaximum('data')}

		# We may or may not be the base of the inheritance tree; usually we are not,
		# but occasionally (mostly in tests) we are
		try:
			super(ModDateTrackingObject, self).__setstate__(data)
		except AttributeError:
			self.__dict__.clear()
			self.__dict__.update(data)

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

class CreatedModDateTrackingObject(ModDateTrackingObject,
								   nti_interfaces.DCTimesLastModifiedMixin):
	""" Adds the `creator` and `createdTime` attributes. """
	def __init__( self, *args, **kwargs ):
		self.createdTime = time.time()
		self.updateLastModIfGreater( self.createdTime )

		super(CreatedModDateTrackingObject,self).__init__( *args, **kwargs )

		# Some of our subclasses have class attributes for fixed creators.
		# don't override those unless we have to
		if not hasattr(self, 'creator'):
			try:
				self.creator = None
			except AttributeError:
				# A read-only property in the class dict that
				# isn't available yet
				pass


class PersistentCreatedModDateTrackingObject(CreatedModDateTrackingObject,PersistentPropertyHolder):
	# order of inheritance matters; if Persistent is first, we can't have our own __setstate__;
	# only subclasses can
	pass


# Commented out while we check for this case
#zope.deferredimport.deprecatedFrom(
#	"The implementation here was broken and exists only for BWC",
#	"nti.dataserver.container",
#	"_CaseInsensitiveKey")


collections.Mapping.register( BTrees.OOBTree.OOBTree )

# See the notes in nti.dataserver.containers package. It's not safe to subclass btrees.
# Consider zc.dict if necessary

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

# XXX
# For BWC, we apply these properties to the base class too,
# but the implementation is not correct as they do not get updated...
_PersistentExternalizableList.__bases__ = (PersistentCreatedModDateTrackingObject,) + _PersistentExternalizableList.__bases__

class PersistentExternalizableWeakList(_PersistentExternalizableWeakList,
									   PersistentCreatedModDateTrackingObject):

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


# These were very bad ideas that didn't work cleanly because
# they tried to store attributes on the BTree itself, which
# doesn't work. We define these deprecated aliases...the
# implementation isn't quite the same but the pickles should basically
# be compatible and work as expected.
zope.deferredimport.deprecatedFrom(
	"Use the container classes instead",
	"nti.dataserver.containers",
	"ModDateTrackingBTreeContainer",
	"KeyPreservingCaseInsensitiveModDateTrackingBTreeContainer")

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



class _VolatileFunctionProperty(PropertyHoldingPersistent):

	def __init__(self, volatile_name, default=_noop):
		self.volatile_name = volatile_name
		self.default = default

	def __get__(self, instance, owner):
		if instance is None:
			return self

		return getattr(instance, self.volatile_name, self.default)

	def __set__(self, instance, value):
		setattr(instance, self.volatile_name, value)

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

	afterAddContainedObject = _VolatileFunctionProperty('_v_afterAdd')

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

	afterDeleteContainedObject = _VolatileFunctionProperty('_v_afterDel')

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

	afterGetContainedObject = _VolatileFunctionProperty('_v_afterGet')

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
				if loc_interfaces.ILocation.providedBy(container) and container.__parent__ is self)

	def __repr__( self ):
		return "<%s size: %s name: %s>" % (self.__class__.__name__, len(self.containers), self.__name__)


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

class AbstractCaseInsensitiveNamedLastModifiedBTreeContainer(container.CaseInsensitiveLastModifiedBTreeContainer,
															 AbstractNamedLastModifiedBTreeContainer):
	pass

zope.deferredimport.deprecatedFrom(
	"Code should not access this directly."
	" The only valid use is existing ZODB objects",
	"nti.zodb.minmax",
	"MergingCounter")
zope.deferredimport.deprecatedFrom(
	"Code should not access this directly."
	" The only valid use is existing ZODB objects",
	"nti.externalization.datastructures",
	"LocatedExternalList",
	"LocatedExternalDict",
	"ExternalizableDictionaryMixin",
	"ExternalizableInstanceDict")
zope.deferredimport.deprecatedFrom(
	"Code should not access this directly."
	" The only valid use is existing ZODB objects",
	"nti.externalization.persistence",
	"PersistentExternalizableDictionary",
	"PersistentExternalizableList")
