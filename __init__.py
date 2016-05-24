#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

# Note that we're not exporting anything by importing it.
# This helps reduce the chances of import cycles

from nti.traversal import monkey as traversing_patch_on_import
traversing_patch_on_import.patch()
del traversing_patch_on_import

from zope.i18nmessageid import MessageFactory
MessageFactory = MessageFactory('nti.dataserver')
