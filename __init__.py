#!/usr/bin/env python

# Note that we're not exporting anything by importing it.
# This helps reduce the chances of import cycles


from nti.monkey import traversing_patch_on_import
traversing_patch_on_import.patch()
del traversing_patch_on_import

import zope.i18nmessageid
MessageFactory = zope.i18nmessageid.MessageFactory('nti.dataserver')
