#!/usr/bin/env python

# Note that we're not exporting anything by importing it.
# This helps reduce the chances of import cycles


from nti.monkey import traversing_patch_on_import
traversing_patch_on_import.patch()
del traversing_patch_on_import
