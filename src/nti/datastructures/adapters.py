#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from nti.links.interfaces import ILink

from nti.externalization.interfaces import INonExternalizableReplacer

logger = __import__('logging').getLogger(__name__)


@component.adapter(ILink)
@interface.implementer(INonExternalizableReplacer)
class LinkNonExternalizableReplacer(object):
    """
    We expect higher levels to handle links, so we let them through.
    """

    def __init__(self, *args):
        pass

    def __call__(self, link):
        return link
