#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import not_none
from hamcrest import instance_of
from hamcrest import assert_that
from hamcrest import has_property
from hamcrest import greater_than
from hamcrest import same_instance
from hamcrest import greater_than_or_equal_to

import pickle
import unittest

import fudge

from ZODB.interfaces import IBroken
from ZODB.interfaces import IConnection

from ZODB.POSException import POSError

from zope import interface

from zope.component.factory import Factory

from zope.location.interfaces import IContained as IZContained

from nti.coremetadata.interfaces import IContained
from nti.coremetadata.interfaces import IHTC_NEW_FACTORY

from nti.coremetadata.mixins import ZContainedMixin

from nti.datastructures.datastructures import isSyntheticKey
from nti.datastructures.datastructures import ContainedStorage
from nti.datastructures.datastructures import VolatileFunctionProperty
from nti.datastructures.datastructures import ContainedObjectValueError
from nti.datastructures.datastructures import check_contained_object_for_storage
from nti.datastructures.datastructures import AbstractNamedLastModifiedBTreeContainer

from nti.dublincore.datastructures import CreatedModDateTrackingObject
from nti.dublincore.datastructures import PersistentCreatedModDateTrackingObject

from nti.datastructures.tests import WithMockDS
from nti.datastructures.tests import SharedConfiguringTestLayer

from nti.datastructures.tests import mock_db_trans

from nti.externalization.interfaces import StandardExternalFields

from nti.externalization.persistence import PersistentExternalizableList

from nti.ntiids.oids import to_external_ntiid_oid


class UniqueKeyPersistentContained(ZContainedMixin,
                                   PersistentCreatedModDateTrackingObject):
    containerId = 'foo'

    def to_container_key(self):
        return "unique-key"


class SampleContained(CreatedModDateTrackingObject, ZContainedMixin):

    def to_container_key(self):
        return to_external_ntiid_oid(self, default_oid=str(id(self)))


class SamplePersistentContained(ZContainedMixin, PersistentCreatedModDateTrackingObject):

    def to_container_key(self):
        return to_external_ntiid_oid(self, default_oid=str(id(self)))


class TestContainedStorage(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_idempotent_add_even_when_wrapped(self):
        cs = ContainedStorage(weak=True)
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)

        # And again with no problems
        cs.addContainedObject(obj)

        # But a new one breaks
        old_id = obj.id
        obj = SampleContained()
        obj.containerId = u'foo'
        obj.id = old_id
        with self.assertRaises(KeyError):
            cs.addContainedObject(obj)

        with self.assertRaises(ValueError):
            cs.addContainer('foo', None)

        with self.assertRaises(TypeError):
            cs.addContainer('foo2', None)

        with self.assertRaises(KeyError):
            cs.deleteContainer('foo2')

    def test_container_type(self):
        # Do all the operations work with dictionaries?
        cs = ContainedStorage(containerType=dict)
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.getContainer('foo'),
                    instance_of(dict))
        assert_that(obj.id, not_none())

        lm = cs.lastModified

        assert_that(cs.deleteContainedObject('foo', obj.id),
                    same_instance(obj))
        assert_that(cs.lastModified, greater_than(lm))
        # container stays around
        assert_that('foo', is_in(cs))

        assert_that(cs.deleteEqualContainedObject(obj), is_(none()))

    def test_pickle(self):
        cs = ContainedStorage()
        del cs.set_ids
        data = pickle.dumps(cs)
        new_cs = pickle.loads(data)
        assert_that(new_cs, has_property('set_ids', True))

    def test_mixed_container_types(self):
        # Should work with the default containerType,
        # plus inserted containers that don't share the same
        # inheritance tree.
        cs = ContainedStorage(containers={u'a': dict()})
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        obj = SampleContained()
        obj.containerId = u'a'
        cs.addContainedObject(obj)

        cs.getContainedObject('foo', '0')
        cs.getContainedObject('a', '0')

    def test_list_container(self):
        cs = ContainedStorage(
            create=True, containerType=PersistentExternalizableList)
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.getContainedObject('foo', 0), is_(obj))
        assert_that(cs.getContainedObject('foo', 1), is_(none()))

    def test_last_modified(self):
        cs = ContainedStorage()
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.lastModified, is_not(0))
        assert_that(cs.lastModified, is_(cs.getContainer('foo').lastModified))

    def test_delete_contained_updates_lm(self):
        cs = ContainedStorage(containerType=PersistentExternalizableList)
        obj = SampleContained()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        lm_add = cs.lastModified
        assert_that(cs.lastModified, is_not(0))
        assert_that(cs.lastModified, is_(cs.getContainer('foo').lastModified))

        # Reset
        cs.getContainer('foo').lastModified = 42
        cs.deleteContainedObject(obj.containerId, obj.id)

        assert_that(cs.lastModified, is_(greater_than_or_equal_to(lm_add)))

    def test_isSyntheticKey(self):
        assert_that(isSyntheticKey(StandardExternalFields.OID),
                    is_(True))

    def test_valueError(self):
        class FakeContained(object):
            def __repr__(self, *args, **kwargs):
                raise Exception
        ContainedObjectValueError('xx', FakeContained())

    def test_check_contained_object_for_storage(self):
        class FakeContained(object):
            containerId = None

        contained = FakeContained()
        with self.assertRaises(ContainedObjectValueError):
            check_contained_object_for_storage(contained)

        interface.alsoProvides(contained, IZContained)
        with self.assertRaises(ContainedObjectValueError):
            check_contained_object_for_storage(contained)

        interface.alsoProvides(contained, IContained)
        with self.assertRaises(ContainedObjectValueError):
            check_contained_object_for_storage(contained)

    def test_volatile_property(self):

        class C(object):
            prop = VolatileFunctionProperty('_v_prop')

        c = C()
        c.prop = lambda x: x
        assert_that(c, has_property('_v_prop', is_not(none())))

    @WithMockDS
    def test_connection(self):
        with mock_db_trans() as conn:
            cs = ContainedStorage()
            conn.add(cs)
            container = cs.getOrCreateContainer('foo')
            assert_that(IConnection(container, None), is_not(none()))

    def test_htc_factory(self):

        class ITest(IZContained):
            pass

        @interface.implementer(ITest)
        class Test(object):
            def __init__(self, *args):
                pass

        # pylint: disable=no-value-for-parameter
        ITest.setTaggedValue(IHTC_NEW_FACTORY,
                             Factory(Test, interfaces=(ITest,)))

        class TestContainer(AbstractNamedLastModifiedBTreeContainer):
            container_name = "test_container"
            contained_type = ITest

        containers = ContainedStorage(containers={
            "test_container": TestContainer()}
        )
        result = containers.maybeCreateContainedObjectWithType(
            "test_container", None
        )
        assert_that(result, is_(Test))

    @WithMockDS
    @fudge.patch('nti.datastructures.datastructures.to_external_ntiid_oid')
    def test_add_container_object(self, mock_te):
        # no id contained
        cs = ContainedStorage(set_ids=False,
                              create=True,
                              containerType=PersistentExternalizableList)

        class NoIdContained(ZContainedMixin):
            containerId = 'foo'

        with self.assertRaises(ContainedObjectValueError):
            cs.addContainedObject(NoIdContained())

        # same unique id
        mock_te.is_callable().returns('xyz')
        with mock_db_trans() as conn:
            cs = ContainedStorage(create=True)
            conn.add(cs)
            cs.addContainedObject(UniqueKeyPersistentContained())

            # adding the same get a new id
            c = UniqueKeyPersistentContained()
            cs.addContainedObject(c)
            assert_that(c, has_property('id', is_('xyz')))

            mock_te.is_callable().returns(None)
            with self.assertRaises(ContainedObjectValueError):
                cs.addContainedObject(UniqueKeyPersistentContained())

    @fudge.patch('nti.datastructures.datastructures.ContainedStorage.doRemoveFromContainer')
    def test_delete_equal_contained_object(self, mock_re):
        cs = ContainedStorage(create=True)
        assert_that(cs.deleteEqualContainedObject(None),
                    is_(none()))
        obj = SamplePersistentContained()
        obj.containerId = u'foo'
        assert_that(cs.deleteEqualContainedObject(obj),
                    is_(none()))
        cs.addContainedObject(obj)
        mock_re.is_callable().raises(TypeError)
        with self.assertRaises(TypeError):
            cs.deleteEqualContainedObject(obj)

        cs = ContainedStorage(create=True,
                              weak=True,
                              containerType=PersistentExternalizableList)
        # add duplicates
        cs.addContainedObject(obj)
        cs.addContainedObject(obj)
        cs.addContainedObject(SamplePersistentContained(containerId=u'foo'))
        # delete should leave last object
        cs.deleteEqualContainedObject(obj)

    def test_get_contained_object(self):
        cs = ContainedStorage(create=True)
        assert_that(cs.getContainedObject('foo', 'id'),
                    is_(none()))

    def test_clean_broken(self):
        cs = ContainedStorage(create=True)

        def _p_activate(*unused_args):
            raise POSError()

        broken = SampleContained(containerId='foo')
        interface.alsoProvides(broken, IBroken)
        cs.addContainedObject(broken)

        bad = SampleContained(containerId='foo')
        cs.addContainedObject(bad)
        # pylint: disable=attribute-defined-outside-init
        bad._p_activate = _p_activate

        assert_that(cs.cleanBroken(), is_(2))
