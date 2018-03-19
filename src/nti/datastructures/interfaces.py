#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class

from zope import interface

from zope.container.interfaces import IContainer


class IHomogeneousTypeContainer(IContainer):
    """
    Things that only want to contain items of a certain type.
    In some cases, an object of this type would be specified
    in an interface as a :class:`zope.schema.List` with a single
    `value_type`.
    """

    contained_type = interface.Attribute(
        """
        The type of objects in the container. May be an Interface type
        or a class type. There should be a ZCA factory to create instances
        of this type associated as tagged data on the type at :data:IHTC_NEW_FACTORY
        """)


IHTC_NEW_FACTORY = 'nti.dataserver.interfaces.IHTCNewFactory'  # BWC
