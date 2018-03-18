#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import assert_that

import unittest

from nti.datastructures.adapters import LinkNonExternalizableReplacer

from nti.datastructures.tests import SharedConfiguringTestLayer


class TestAdapters(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_replacer(self):
        fake = object()
        replacer = LinkNonExternalizableReplacer()
        assert_that(replacer(fake), is_(fake))
