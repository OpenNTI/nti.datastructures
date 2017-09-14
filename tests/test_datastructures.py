#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import not_none
from hamcrest import instance_of
from hamcrest import assert_that
from hamcrest import greater_than
from hamcrest import has_property
from hamcrest import same_instance
from hamcrest import greater_than_or_equal_to

from nose.tools import assert_raises

import unittest

from nti.coremetadata.mixins import ZContainedMixin

from nti.datastructures.datastructures import ContainedStorage
from nti.datastructures.datastructures import LastModifiedCopyingUserList

from nti.dublincore.datastructures import CreatedModDateTrackingObject

from nti.externalization.externalization import toExternalObject

from nti.ntiids.oids import to_external_ntiid_oid

from nti.datastructures.tests import SharedConfiguringTestLayer


class TestLastModifiedCopyingUserList(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_extend(self):
        l = LastModifiedCopyingUserList()
        assert_that(l.lastModified, is_(0))

        l.lastModified = -1
        l.extend([])
        assert_that(l.lastModified, is_(-1))

        l2 = LastModifiedCopyingUserList([1, 2, 3])
        assert_that(l2, has_property('lastModified', 0))
        l.extend(LastModifiedCopyingUserList([1, 2, 3]))
        assert_that(l.lastModified, is_(0))
        assert_that(l, is_([1, 2, 3]))

    def test_plus(self):
        l = LastModifiedCopyingUserList()
        l.lastModified = -1
        l += []
        assert_that(l.lastModified, is_(-1))

        l += LastModifiedCopyingUserList([1, 2, 3])
        assert_that(l.lastModified, is_(0))
        assert_that(l, is_([1, 2, 3]))


import persistent

from nti.externalization.persistence import PersistentExternalizableList
from nti.externalization.persistence import PersistentExternalizableWeakList


class TestPersistentExternalizableWeakList(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_plus_extend(self):
        class C(persistent.Persistent):
            pass
        c1 = C()
        c2 = C()
        c3 = C()
        l = PersistentExternalizableWeakList()
        l += [c1, c2, c3]
        assert_that(l, is_([c1, c2, c3]))
        assert_that([c1, c2, c3], is_(l))

        # Adding things that are already weak refs.
        l += l
        assert_that(l, is_([c1, c2, c3, c1, c2, c3]))

        l = PersistentExternalizableWeakList()
        l.extend([c1, c2, c3])
        assert_that(l, is_([c1, c2, c3]))
        assert_that(l, is_(l))


class TestContainedStorage(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    class C(CreatedModDateTrackingObject, ZContainedMixin):

        def to_container_key(self):
            return to_external_ntiid_oid(self, default_oid=str(id(self)))

    def test_idempotent_add_even_when_wrapped(self):
        cs = ContainedStorage(weak=True)
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)

        # And again with no problems
        cs.addContainedObject(obj)

        # But a new one breaks
        old_id = obj.id
        obj = self.C()
        obj.containerId = u'foo'
        obj.id = old_id
        with assert_raises(KeyError):
            cs.addContainedObject(obj)

    def test_container_type(self):
        # Do all the operations work with dictionaries?
        cs = ContainedStorage(containerType=dict)
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.getContainer('foo'), instance_of(dict))
        assert_that(obj.id, not_none())

        lm = cs.lastModified

        assert_that(cs.deleteContainedObject('foo', obj.id),
                    same_instance(obj))
        assert_that(cs.lastModified, greater_than(lm))
        # container stays around
        assert_that('foo', is_in(cs))

    def test_mixed_container_types(self):
        # Should work with the default containerType,
        # plus inserted containers that don't share the same
        # inheritance tree.
        cs = ContainedStorage(containers={u'a': dict()})
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        obj = self.C()
        obj.containerId = u'a'
        cs.addContainedObject(obj)

        cs.getContainedObject('foo', '0')
        cs.getContainedObject('a', '0')

    def test_list_container(self):
        cs = ContainedStorage(containerType=PersistentExternalizableList)
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.getContainedObject('foo', 0), is_(obj))

    def test_last_modified(self):
        cs = ContainedStorage()
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        assert_that(cs.lastModified, is_not(0))
        assert_that(cs.lastModified, is_(cs.getContainer('foo').lastModified))

    def test_delete_contained_updates_lm(self):
        cs = ContainedStorage(containerType=PersistentExternalizableList)
        obj = self.C()
        obj.containerId = u'foo'
        cs.addContainedObject(obj)
        lm_add = cs.lastModified
        assert_that(cs.lastModified, is_not(0))
        assert_that(cs.lastModified, is_(cs.getContainer('foo').lastModified))

        # Reset
        cs.getContainer('foo').lastModified = 42
        cs.deleteContainedObject(obj.containerId, obj.id)

        assert_that(cs.lastModified, is_(greater_than_or_equal_to(lm_add)))


from zope import interface, component

from nti.externalization.interfaces import IExternalObject
from nti.externalization.interfaces import IExternalObjectDecorator


class TestToExternalObject(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_decorator(self):

        class ITest(interface.Interface):
            pass

        @interface.implementer(ITest, IExternalObject)
        class Test(object):

            def toExternalObject(self, **kwargs):
                return {}

        test = Test()
        assert_that(toExternalObject(test), is_({}))

        @interface.implementer(IExternalObjectDecorator)
        class Decorator(object):

            def __init__(self, o):
                pass

            def decorateExternalObject(self, obj, result):
                result['test'] = obj

        component.provideSubscriptionAdapter(Decorator, adapts=(ITest,))
        assert_that(toExternalObject(test), is_({'test': test}))
