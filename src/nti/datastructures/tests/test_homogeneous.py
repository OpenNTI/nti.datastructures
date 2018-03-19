#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import has_key
from hamcrest import assert_that

import unittest

from zope import interface

from zope.component.factory import Factory

from zope.container.interfaces import InvalidItemType

from zope.location.interfaces import IContained as IZContained

from nti.coremetadata.interfaces import IHTC_NEW_FACTORY

from nti.coremetadata.mixins import ZContainedMixin

from nti.datastructures.datastructures import AbstractNamedLastModifiedBTreeContainer

from nti.datastructures.tests import SharedConfiguringTestLayer


class TestHomogeneousTypeContainer(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_container(self):

        class ITest(IZContained):
            pass

        @interface.implementer(ITest)
        class Test(ZContainedMixin):
            pass

        # pylint: disable=no-value-for-parameter
        ITest.setTaggedValue(IHTC_NEW_FACTORY,
                             Factory(Test, interfaces=(ITest,)))

        class TestContainer(AbstractNamedLastModifiedBTreeContainer):
            container_name = "test_container"
            contained_type = ITest

        container = TestContainer()
        with self.assertRaises(InvalidItemType):
            container['foo'] = ZContainedMixin()

        container['foo'] = Test()
        assert_that(container, has_key('foo'))
