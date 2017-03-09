#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Datatypes and datatype handling.

.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import logging
import weakref
import collections

from zope import interface

from zope.container.constraints import checkObject
from zope.container.interfaces import InvalidItemType

from zope.location import locate as loc_locate

from zope.location.interfaces import ILocation
from zope.location.interfaces import ISublocations
from zope.location.interfaces import IContained as IZContained

from ZODB.interfaces import IBroken

from ZODB.POSException import POSError

from BTrees.OOBTree import OOBTree

from persistent.wref import WeakRef

from nti.base.interfaces import ILastModified

from nti.containers.containers import LastModifiedBTreeContainer
from nti.containers.containers import CheckingLastModifiedBTreeContainer
from nti.containers.containers import CaseInsensitiveLastModifiedBTreeContainer

from nti.coremetadata.interfaces import IHTC_NEW_FACTORY

from nti.coremetadata.interfaces import IContained
from nti.coremetadata.interfaces import INamedContainer
from nti.coremetadata.interfaces import IHomogeneousTypeContainer

from nti.dublincore.time_mixins import ModDateTrackingObject

from nti.externalization.interfaces import StandardInternalFields
from nti.externalization.interfaces import StandardExternalFields

from nti.externalization.oids import to_external_ntiid_oid

from nti.zodb.persistentproperty import PersistentPropertyHolder
from nti.zodb.persistentproperty import PropertyHoldingPersistent


def _syntheticKeys():
    return (StandardExternalFields.ID,
            StandardExternalFields.OID,
            StandardExternalFields.CLASS,
            StandardExternalFields.CREATOR,
            StandardExternalFields.CONTAINER_ID,
            StandardExternalFields.LAST_MODIFIED)


def _isMagicKey(key):
    """
    For our mixin objects that have special keys, defines
    those keys that are special and not settable by the user.
    """
    return key in _syntheticKeys()
isSyntheticKey = _isMagicKey


# For speed and use in this function, we declare an 'inline'-able attribute
_magic_keys = set(_syntheticKeys())

mapping_register = getattr(collections.Mapping, 'register')
mapping_register(OOBTree)


# See the notes in nti.dataserver.containers package. It's not safe to subclass btrees.
# Consider zc.dict if necessary
class LastModifiedCopyingUserList(ModDateTrackingObject, list):
    """
    For building up a sequence of lists, keeps the max last modified.
    """

    def extend(self, other):
        super(LastModifiedCopyingUserList, self).extend(other)
        self.updateLastModIfGreater(getattr(other, 'lastModified', self.lastModified))

    def __iadd__(self, other):
        result = super(LastModifiedCopyingUserList, self).__iadd__(other)
        self.updateLastModIfGreater(getattr(other, 'lastModified', self.lastModified))
        return result

    def __reduce__(self):
        raise TypeError("Transient object.")


def _noop(*args): 
    pass


class _ContainedObjectValueError(ValueError):
    """
    A more naturally descriptive exception for contained objects.
    """

    def __init__(self, string, contained=None, **kwargs):
        ctype = type(contained)
        cstr = 'Unable to determine'
        try:
            cstr = repr(contained)
        except Exception as e:
            cstr = '{%s}' % e
        super(_ContainedObjectValueError, self).__init__("%s [type: %s repr %s]%s" % (string, ctype, cstr, kwargs))


def check_contained_object_for_storage(contained):
    if not IContained.providedBy(contained):
        raise _ContainedObjectValueError("Contained object is not " + str(IContained),
                                         contained)

    if not IZContained.providedBy(contained):
        raise _ContainedObjectValueError("Contained object is not " + str(IZContained),
                                         contained)

    if not getattr(contained, 'containerId'):
        raise _ContainedObjectValueError("Contained object has empty containerId",
                                         contained)


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


@interface.implementer(IZContained, ISublocations)
class ContainedStorage(PersistentPropertyHolder, ModDateTrackingObject):
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

    __name__ = None
    __parent__ = None

    # TODO: Remove the containerType argument; nothing except tests uses it now,
    # everything else uses the standard type.
    # That will let us remove the complicated code to do different things based on
    # the type of container.
    def __init__(self, weak=False, create=False, containers=None,
                 containerType=CheckingLastModifiedBTreeContainer,
                 set_ids=True, containersType=OOBTree):
        """
        Creates a new container.

        :param bool weak: If true, we will maintain weak references to contained objects.
        :param object create: A boolean or object value. If it is true, the `creator` property
            of objects added to us will be set. If `create` is a boolean, this `creator` property
            will be set to this object (useful for subclassing). Otherwise, the `creator` property
            will be set to the value of `create`.
        :param dict containers: Initial containers. We do not adopt these containers,
            they may already have a __parent__ (presumably an ancestor of ours as well)
        :param type containerType: The type for each created container. Should be a mapping
            type, and should handle conflicts. The default value only allows comparable keys.
            The type can also be a `list` type, though this use is deprecated and discouraged.
        :param type containersType: The mapping type factory that will hold the containers.
            Default is :class:`ModDateTrackingOOBTree`, another choice is
            :class:`CaseInsensitiveModDateTrackingOOBTree`.
        :param bool set_ids: If true (default) the ``id`` field of newly added objects will be set.
            Otherwise, the must already have an id. Set this to False if the added objects
            are shared (and especially shared in the database.)
        """

        super(ContainedStorage, self).__init__()
        self.containers = containersType()  # read-only, but mutates.
        self.weak = weak  # read-only
        self.create = create  # read-only
        self.containerType = containerType  # read-only
        self.set_ids = set_ids  # read-only
        self._setup()

        for k, v in (containers or {}).iteritems():
            # Notice that we're not using addContainer: these don't
            # become our children.
            self.containers[k] = v

    def _setup(self):
        if self.weak:
            def wrap(obj):
                return WeakRef(obj) if hasattr(obj, '_p_oid') else weakref.ref(obj)

            def unwrap(obj):
                return obj() if obj is not None else None

            self._v_wrap = wrap
            self._v_unwrap = unwrap
        else:
            def wrap(obj): 
                return obj

            def unwrap(obj): 
                return obj

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
        def _put_in_container(c, i, d, orig):
            if isinstance(c, collections.Mapping):
                c[i] = d
            else:
                c.append(d)
                if self.set_ids:
                    try:
                        setattr(orig, StandardInternalFields.ID, unicode(len(c) - 1))
                    except AttributeError:
                        logger.debug("Failed to set id", exc_info=True)

        def _get_in_container(c, i, d=None):
            if isinstance(c, collections.Mapping):
                # BTree containers raise TypeError on a None key
                return c.get(i, d) if i is not None else d
            try:
                return c[int(i)]
            except IndexError:
                return d

        def _remove_in_container(c, d):
            if isinstance(c, collections.Mapping):
                for k, v in c.iteritems():
                    if v == d:
                        del c[k]
                        return v
                raise ValueError(d)
            # Lists. Note that duplicates may have
            # crept in. TODO: We should probably remove them all
            ix = c.index(d)
            d = c[ix]
            c.pop(ix)
            return d

        self._v_putInContainer = _put_in_container
        self._v_getInContainer = _get_in_container
        self._v_removeFromContainer = _remove_in_container

    def _v_wrap(self, obj):
        pass

    def _v_unwrap(self, obj):
        pass

    def _v_create(self, obj):
        pass

    def _v_putInContainer(self, obj, orig):
        pass

    def _v_getInContainer(self, obj, defv=None):
        pass

    def _v_removeFromContainer(self, o):
        """
        Raises ValueError if the object is not in the container"
        """

    def __setstate__(self, dic):
        super(ContainedStorage, self).__setstate__(dic)
        if not hasattr(self, 'set_ids'):
            self.set_ids = True
        self._setup()

    def addContainer(self, containerId, container, locate=True):
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
            raise ValueError('%s already exists' % (containerId))

        if container is None or containerId is None:
            raise TypeError('Container/Id cannot be None')

        self.containers[containerId] = container
        if locate and ILocation.providedBy(container):
            loc_locate(container, self, containerId)

    def deleteContainer(self, containerId):
        """
        Removes an existing container, if one already exists.
        :raises: KeyError If no container exists.
        """
        del self.containers[containerId]

    def getContainer(self, containerId, defaultValue=None):
        """ 
        Given a container ID, returns the existing container, or
        the default value if there is no container. The returned
        value SHOULD NOT be modified. 
        """
        # FIXME: handle unwrapping of the contained objects
        return self.containers.get(containerId, defaultValue)

    def getOrCreateContainer(self, containerId):
        """
        Return a container for the given containerId. If one
        does not already exist, it will be created and stored.
        """
        container = self.containers.get(containerId, None)
        if container is None:
            container = self.containerType()
            if getattr(self, '_p_jar', None) and hasattr(container, '_p_jar'):
                getattr(self, '_p_jar').add(container)
            self.addContainer(containerId, container)
        return container

    def maybeCreateContainedObjectWithType(self, datatype, externalValue):
        """ 
        If we recognize and own the given datatype, creates
        a new default instance and returns it. Otherwise returns
        None. 
        """
        result = None
        container = self.containers.get(datatype)
        if IHomogeneousTypeContainer.providedBy(container):
            factory = container.contained_type.queryTaggedValue(IHTC_NEW_FACTORY)
            if factory:
                result = factory(externalValue)
        return result

    def addContainedObject(self, contained):
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
        check_contained_object_for_storage(contained)
        container = self.getOrCreateContainer(contained.containerId)
        if isinstance(container, collections.Mapping):
            # don't allaw adding a new object on top of an existing one,
            # unless the existing one is broken (migration botched, etc).
            # Be idempotent, though, and ignore the same object (taking
            # wrapping into account)
            if contained.id:
                existing = container.get(contained.id, None)
                if existing is not None:
                    existing = self._v_unwrap(existing)
                    if existing is contained:
                        return existing  # Nothing more do do
                    # OK, so it's not contained. Is it broken?
                    if IBroken not in interface.providedBy(existing):
                        __traceback_info__ = contained, existing
                        raise KeyError("Contained object uses existing ID " + str(contained.id))

        # Save
        if not contained.id and not self.set_ids:
            raise _ContainedObjectValueError("Contained object has no id and we are not allowed to give it one.",
                                             contained)

        # Add to the connection so it can start creating an OID
        # if we are saved, and it is Persistent but unsaved
        if      getattr(self, '_p_jar', None) \
            and getattr(contained, '_p_jar', self) is None:
            getattr(self, '_p_jar').add(contained)

        self._v_create(contained)
        if not contained.id:
            # TODO: Need to allow individual content types some control
            # over this, specifically quizzes. This is a hack for them,
            # which doesn't quite work: they can only generate a good container_key if
            # they already have an ID, and so we don't take this code path
            the_id = None
            if getattr(contained, 'to_container_key', None):
                the_id = contained.to_container_key()
                if      isinstance(container, collections.Mapping) \
                    and container.get(the_id, contained) is not contained:
                    # Don't allow overrwriting
                    the_id = None
            if the_id is None:
                the_id = to_external_ntiid_oid(contained, add_to_intids=True)
            contained.id = the_id

        __traceback_info__ = container, contained.containerId, contained.id
        if contained.id is None:
            raise _ContainedObjectValueError("Unable to determine contained id", 
                                             contained)

        self._v_putInContainer(container,
                               contained.id,
                               self._v_wrap(contained),
                               contained)
        # Synchronize the timestamps
        self._updateContainerLM(container)

        self.afterAddContainedObject(contained)
        return contained

    def _updateContainerLM(self, container):
        self.updateLastMod()
        up = getattr(container, 'updateLastMod', None)
        if callable(up):
            up(self.lastModified)

    afterAddContainedObject = _VolatileFunctionProperty('_v_afterAdd')

    def deleteContainedObject(self, containerId, containedId):
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
        return self.deleteEqualContainedObject(self.getContainedObject(containerId, containedId))

    def deleteEqualContainedObject(self, contained, log_level=logging.DEBUG):
        """
        Given an object contained herein, removes it. Returns the removed
            object, if found, else returns None.

        :param log_level: The level at which we log if we are unable to delete
            the object. If this is expected to be common and harmless, set it lower than DEBUG.
        """
        if contained is None or contained.containerId is None:
            logger.log(log_level,
                       "Unable to delete object equal to None or with no containerId: %s",
                       contained)
            return None
        container = self.containers.get(contained.containerId)
        if container is None:
            logger.log(
                log_level,
                "Unable to delete object we (%r) have no container for: %s (%s) (%s) (%s %r %r %r)",
                self,
                contained.containerId, list(self.containers.keys()),
                self.containers._p_state,
                self.containers._p_jar,
                self.containers._p_oid,
                self.containers._p_serial,
                contained)
            return None

        wrapped = self._v_wrap(contained)  # outside the catch
        try:
            contained = self._v_unwrap(self._v_removeFromContainer(container, wrapped))
        except ValueError:
            logger.log(log_level, 
                       "Failed to find object to delete equal to %s", 
                       contained)
            return None
        except TypeError:
            logger.log(log_level, 
                       "Failed to find object to delete equal to %s", 
                       contained,
                       exc_info=True)
            # Getting here means that we are no longer able to resolve
            # at least one object by OID. Might as well take this opportunity
            # to clear out all the dangling refs. Notice we keep the identical
            # container object though
            # FIXME: This code only works when we're using list containers.
            if isinstance(container, collections.Mapping):
                raise
            cid = getattr(contained, '_p_oid', self) or self
            tmp = list(container)
            del container[:]
            for weak in tmp:
                if    cid == getattr(weak, 'oid', None) \
                   or cid == getattr(weak, '_p_oid', None):
                    continue
                strong = weak if not callable(weak) else weak()
                if strong is not None and strong != contained:
                    container.append(strong)
                else:
                    logger.log(log_level,
                               "Dropping obj by equality/missing during delete %s == %s",
                               strong,
                               contained)
            return None
        else:
            self._updateContainerLM(container)
            self.afterDeleteContainedObject(contained)
            return contained

    afterDeleteContainedObject = _VolatileFunctionProperty('_v_afterDel')

    def getContainedObject(self, containerId, containedId, defaultValue=None):
        """ 
        Given a container ID and an id within that container,
        retreives the designated object, or the default value (None if not
        specified) if the object cannot be found.
        """
        container = self.containers.get(containerId)
        if container is None:
            # our unbound method in the other branch
            # means we cannot cheaply use a default value to the
            # get call.
            result = defaultValue
        else:
            result = self._v_getInContainer(container, 
                                            containedId, 
                                            defaultValue)
        if result is not defaultValue:
            result = self._v_unwrap(result)
            self.afterGetContainedObject(result)
        return result

    afterGetContainedObject = _VolatileFunctionProperty('_v_afterGet')

    def cleanBroken(self):
        result = 0
        for container in self.itervalues():
            is_mapping = isinstance(container, collections.Mapping)
            if not is_mapping:
                continue
            for name, value in list(container.items()):
                try:
                    value = self._v_wrap(value)
                    if value is not None:
                        if IBroken.providedBy(value):
                            result += 1
                            del container[name]
                            logger.warn("Removing broken object %s,%s", 
                                        name,
                                        type(value))
                        elif hasattr(value, '_p_activate'):
                            value._p_activate()
                except POSError:
                    result += 1
                    del container[name]
                    logger.warn("Removing broken object %s,%s",
                                name,
                                type(value))
        return result

    def __iter__(self):
        return iter(self.containers)

    def __contains__(self, val):
        return val in self.containers

    def __getitem__(self, i):
        return self.containers[i]

    def iteritems(self):
        return self.containers.iteritems()

    def itervalues(self):
        return self.containers.itervalues()

    values = itervalues

    def iter_all_contained_objects(self):
        """
        Only works for dict-like containers
        """
        for container in self.itervalues():
            for v in container.values():
                yield v

    def sublocations(self):
        return (container for container
                in self.itervalues()
                # Recall that we could be holding containers given to __init__
                # that we are not the parent of
                if ILocation.providedBy(container) and container.__parent__ is self)

    def __repr__(self):
        return "<%s size: %s name: %s>" % (self.__class__.__name__,
                                           len(self.containers),
                                           self.__name__)


@interface.implementer(IHomogeneousTypeContainer,
                       INamedContainer,
                       ILastModified)
class AbstractNamedLastModifiedBTreeContainer(LastModifiedBTreeContainer):
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

    def __init__(self, *args, **kwargs):
        super(AbstractNamedLastModifiedBTreeContainer, self).__init__(*args, **kwargs)

    def __setitem__(self, key, item):
        # TODO: Finish porting this all over to the constraints in zope.container.
        # That will require specific subtypes for each contained_type (which we already have)
        # We start the process by using checkObject to validate any preconditions
        # that are defined
        checkObject(self, key, item)
        if not self.contained_type.providedBy(item):
            raise InvalidItemType(self, item, (self.contained_type,))
        super(AbstractNamedLastModifiedBTreeContainer, self).__setitem__(key, item)


class AbstractCaseInsensitiveNamedLastModifiedBTreeContainer(CaseInsensitiveLastModifiedBTreeContainer,
                                                             AbstractNamedLastModifiedBTreeContainer):
    pass

import zope.deferredimport
zope.deferredimport.initialize()

zope.deferredimport.deprecatedFrom(
    "Moved to nti.dublincore.time_mixins",
    "nti.dublincore.time_mixins",
    "CreatedAndModifiedTimeMixin")
