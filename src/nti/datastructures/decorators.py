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

from zope.location.interfaces import ILocation

from nti.externalization.externalization import toExternalObject

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.externalization.singleton import Singleton

from nti.links.links import Link

LINKS = StandardExternalFields.LINKS

logger = __import__('logging').getLogger(__name__)


def find_links(self):
    """
    Return a sequence of things that should be thought of as related links to
    a given object, including enclosures and the `links` property.
    :return: A sequence of :class:`interfaces.ILink` objects.
    """
    _links = []
    if callable(getattr(self, 'iterenclosures', None)):
        _links = [
            Link(enclosure, rel='enclosure')
            for enclosure in self.iterenclosures()
        ]
    _links.extend(getattr(self, 'links', ()))
    return _links


@component.adapter(object)
@interface.implementer(IExternalMappingDecorator)
class LinkDecorator(Singleton):

    def decorateExternalMapping(self, context, result):
        # We have no way to know what order these will be
        # called in, so we must preserve anything that exists
        orig_links = result.get(StandardExternalFields.LINKS, ())
        _links = find_links(context)
        _links = [toExternalObject(l) for l in _links if l]
        _links = [l for l in _links if l]  # strip none
        if _links:
            _links = sorted(_links)
            for link in _links:
                interface.alsoProvides(link, ILocation)
                link.__name__ = ''
                link.__parent__ = context
        _links.extend(orig_links)
        if _links:
            result[LINKS] = _links
