#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

## Note that we're not exporting anything by importing it.
## This helps reduce the chances of import cycles

# XXXX: Because we are now a namespace package, we have
# no guarantee this file will ever actually be imported!
from nti.traversal import monkey as traversing_patch_on_import
traversing_patch_on_import.patch()
del traversing_patch_on_import


__import__('pkg_resources').declare_namespace(__name__)
