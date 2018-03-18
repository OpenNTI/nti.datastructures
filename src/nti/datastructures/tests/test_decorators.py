#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that

import unittest

from nti.datastructures.decorators import LinkDecorator

from nti.datastructures.tests import SharedConfiguringTestLayer

from nti.externalization.interfaces import StandardExternalFields

LINKS = StandardExternalFields.LINKS


class TestDecorators(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_decorator(self):

        class Contained(object):
            __parent__ = None
            name = __name__ = "Name"

        class Container(object):

            def iterenclosures(self):
                c = Contained()
                c.__parent__ = self
                return (c,)

        result = {}
        LinkDecorator().decorateExternalMapping(Container(), result)
        assert_that(result,
                    has_entry(LINKS, has_length(1)))
