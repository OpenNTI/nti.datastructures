#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import assert_that

import unittest

from zope import component
from zope import interface

import persistent

from nti.datastructures.tests import SharedConfiguringTestLayer

from nti.externalization.externalization import toExternalObject

from nti.externalization.interfaces import IExternalObject
from nti.externalization.interfaces import IExternalObjectDecorator

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


class TestToExternalObject(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_decorator(self):
        # pylint: disable=inherit-non-class
        class ITest(interface.Interface):
            pass

        @interface.implementer(ITest, IExternalObject)
        class Test(object):

            def toExternalObject(self, **unused_kwargs):
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
