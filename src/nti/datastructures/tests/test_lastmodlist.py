#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ

from hamcrest import is_
from hamcrest import assert_that
from hamcrest import has_property

import pickle
import unittest

from nti.datastructures.datastructures import LastModifiedCopyingUserList

from nti.datastructures.tests import SharedConfiguringTestLayer


class TestLastModifiedCopyingUserList(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    def test_extend(self):
        l = LastModifiedCopyingUserList()
        assert_that(l.lastModified, is_(0))

        l.lastModified = -1
        l.extend(())
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

    def test_pickle(self):
        user_list = LastModifiedCopyingUserList()
        with self.assertRaises(TypeError):
            pickle.dumps(user_list)
