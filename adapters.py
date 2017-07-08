#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from nti.dataserver.interfaces import ILink

from nti.externalization.interfaces import INonExternalizableReplacer


@component.adapter(ILink)
@interface.implementer(INonExternalizableReplacer)
class LinkNonExternalizableReplacer(object):
    """
    We expect higher levels to handle links, so we let them through.
    """

    def __init__(self, o):
        pass

    def __call__(self, link):
        return link
