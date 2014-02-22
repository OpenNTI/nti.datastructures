#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""


$Id$
"""

from __future__ import print_function, unicode_literals, absolute_import
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

#disable: accessing protected members, too many methods
#pylint: disable=W0212,R0904

from hamcrest import assert_that
from hamcrest import is_
from hamcrest import instance_of
from hamcrest import is_in
from hamcrest import not_none
from hamcrest import is_not
from hamcrest import greater_than

does_not = is_not
from hamcrest import greater_than_or_equal_to
from hamcrest import same_instance
from hamcrest import has_key
from hamcrest.library import has_property as has_attr
import unittest
from nose.tools import assert_raises

from nti.testing.base import AbstractTestBase




import persistent
from nti.externalization.oids import to_external_ntiid_oid
import nti.deprecated

with nti.deprecated.hiding_warnings():
	from nti.dataserver.datastructures import ModDateTrackingObject
	from nti.dataserver.datastructures import LastModifiedCopyingUserList
	from nti.dataserver.datastructures import ContainedStorage, ZContainedMixin, CreatedModDateTrackingObject

	from nti.externalization.externalization import toExternalObject
	from nti.externalization.oids import toExternalOID


	from nti.testing.matchers import validly_provides as verifiably_provides
	from . import mock_dataserver
	from nti.dataserver import contenttypes
	from nti.dataserver import interfaces as nti_interfaces
	from nti.ntiids import ntiids

class TestMisc(AbstractTestBase):

	def test_containedmixins(self):
		cm = ZContainedMixin()
		assert_that( cm, verifiably_provides( nti_interfaces.IContained ) )
		assert_that( cm, verifiably_provides( nti_interfaces.IZContained ) )

	def test_moddatetrackingobject_oldstates(self):
		mto = ModDateTrackingObject()
		assert_that( mto.lastModified, is_( 0 ) )
		assert_that( mto.__dict__, does_not( has_key( '_lastModified' ) ) )

		# old state
		mto.__setstate__( {'_lastModified': 32 } )
		assert_that( mto.lastModified, is_( 32 ) )

		# updates dynamically
		mto.updateLastMod(42)
		assert_that( mto.lastModified, is_( 42 ) )
		assert_that( mto._lastModified, has_attr( 'value', 42 ) )

		# missing entirely
		del mto._lastModified
		assert_that( mto.lastModified, is_( 0 ) )
		mto.updateLastMod( 42 )
		assert_that( mto.lastModified, is_( 42 ) )
		assert_that( mto._lastModified, has_attr( 'value', 42 ) )

		mto._lastModified.__getstate__()

class TestLastModifiedCopyingUserList(AbstractTestBase):

	def test_extend( self ):
		l = LastModifiedCopyingUserList()
		assert_that( l.lastModified, is_( 0 ) )

		l.lastModified = -1
		l.extend( [] )
		assert_that( l.lastModified, is_( -1 ) )

		l2 = LastModifiedCopyingUserList([1,2,3])
		assert_that( l2, has_attr( 'lastModified', 0 ) )
		l.extend( LastModifiedCopyingUserList([1,2,3]) )
		assert_that( l.lastModified, is_( 0 ) )
		assert_that( l, is_([1,2,3]) )

	def test_plus( self ):
		l = LastModifiedCopyingUserList()
		l.lastModified = -1
		l += []
		assert_that( l.lastModified, is_( -1 ) )

		l += LastModifiedCopyingUserList([1,2,3])
		assert_that( l.lastModified, is_( 0 ) )
		assert_that( l, is_([1,2,3]) )
from nti.externalization.persistence import PersistentExternalizableWeakList, PersistentExternalizableList
class TestPersistentExternalizableWeakList(AbstractTestBase):

	def test_plus_extend( self ):
		class C( persistent.Persistent ): pass
		c1 = C()
		c2 = C()
		c3 = C()
		l = PersistentExternalizableWeakList()
		l += [c1, c2, c3]
		assert_that( l, is_( [c1, c2, c3] ) )
		assert_that( [c1, c2, c3], is_(l) )

		# Adding things that are already weak refs.
		l += l
		assert_that( l, is_( [c1, c2, c3, c1, c2, c3] ) )

		l = PersistentExternalizableWeakList()
		l.extend( [c1, c2, c3] )
		assert_that( l, is_( [c1, c2, c3] ) )
		assert_that( l, is_(l) )

class TestContainedStorage(unittest.TestCase):

	layer = mock_dataserver.SharedConfiguringTestLayer

	class C(CreatedModDateTrackingObject,ZContainedMixin):
		def to_container_key(self):
			return to_external_ntiid_oid( self, default_oid=str(id(self)) )

	def test_idempotent_add_even_when_wrapped(self):
		cs = ContainedStorage( weak=True )
		obj = self.C()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )

		# And again with no problems
		cs.addContainedObject( obj )

		# But a new one breaks
		old_id = obj.id
		obj = self.C()
		obj.containerId = 'foo'
		obj.id = old_id
		with assert_raises( KeyError ):
			cs.addContainedObject( obj )

	def test_container_type(self):
		# Do all the operations work with dictionaries?
		cs = ContainedStorage( containerType=dict )
		obj = self.C()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )
		assert_that( cs.getContainer( 'foo' ), instance_of( dict ) )
		assert_that( obj.id, not_none() )

		lm = cs.lastModified

		assert_that( cs.deleteContainedObject( 'foo', obj.id ), same_instance( obj ) )
		assert_that( cs.lastModified, greater_than( lm ) )
		# container stays around
		assert_that( 'foo', is_in( cs ) )

	def test_mixed_container_types( self ):
		# Should work with the default containerType,
		# plus inserted containers that don't share the same
		# inheritance tree.
		cs = ContainedStorage( containers={'a': dict()} )
		obj = self.C()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )
		obj = self.C()
		obj.containerId = 'a'
		cs.addContainedObject( obj )

		cs.getContainedObject( 'foo', '0' )
		cs.getContainedObject( 'a', '0' )

	def test_list_container( self ):
		cs = ContainedStorage( containerType=PersistentExternalizableList )
		obj = self.C()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )
		assert_that( cs.getContainedObject( 'foo', 0 ), is_( obj ) )

	def test_last_modified( self ):
		cs = ContainedStorage()
		obj = self.C()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )
		assert_that( cs.lastModified, is_not( 0 ) )
		assert_that( cs.lastModified, is_( cs.getContainer( 'foo' ).lastModified ) )

	def test_delete_contained_updates_lm( self ):
		cs = ContainedStorage( containerType=PersistentExternalizableList )
		obj = self.C()
		obj.containerId = u'foo'
		cs.addContainedObject( obj )
		lm_add = cs.lastModified
		assert_that( cs.lastModified, is_not( 0 ) )
		assert_that( cs.lastModified, is_( cs.getContainer( 'foo' ).lastModified ) )

		# Reset
		cs.getContainer( 'foo' ).lastModified = 42
		cs.deleteContainedObject( obj.containerId, obj.id )

		assert_that( cs.lastModified, is_( greater_than_or_equal_to( lm_add ) ) )

	@mock_dataserver.WithMockDSTrans
	def test_id_is_ntiid(self):
		cs = ContainedStorage( )
		mock_dataserver.current_transaction.add( cs )

		obj = contenttypes.Note()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )

		assert_that( obj._p_jar, is_( cs._p_jar ) )
		assert_that( obj._p_jar, is_( not_none() ) )
		ntiids.validate_ntiid_string( obj.id )

		# Without a creator, we get the system principal as the provider
		ntiid = ntiids.get_parts( obj.id )
		assert_that( ntiid.provider, is_( nti_interfaces.SYSTEM_USER_NAME ) )
		assert_that( ntiid.nttype, is_( 'OID' ) )
		assert_that( ntiid.specific, is_( toExternalOID( obj ) ) )

		# with a creator, we get the creator as the provider
		cs = ContainedStorage( create='sjohnson@nextthought.com' )
		mock_dataserver.current_transaction.add( cs )

		obj = contenttypes.Note()
		obj.containerId = 'foo'
		cs.addContainedObject( obj )
		assert_that( obj._p_jar, is_( cs._p_jar ) )
		assert_that( obj._p_jar, is_( not_none() ) )
		ntiids.validate_ntiid_string( obj.id )

		ntiid = ntiids.get_parts( obj.id )
		assert_that( ntiid.provider, is_( 'sjohnson@nextthought.com' ) )
		assert_that( ntiid.nttype, is_( ntiids.TYPE_OID ) )
		assert_that( ntiid.specific, is_( toExternalOID( obj ) ) )

does_not = is_not


from zope import interface, component
from nti.externalization.interfaces import IExternalObject, IExternalObjectDecorator


class TestToExternalObject(unittest.TestCase):

	layer = mock_dataserver.SharedConfiguringTestLayer

	def test_decorator(self):
		class ITest(interface.Interface): pass
		class Test(object):
			interface.implements(ITest,IExternalObject)

			def toExternalObject(self, **kwargs):
				return {}

		test = Test()

		assert_that( toExternalObject( test ), is_( {} ) )

		class Decorator(object):
			interface.implements(IExternalObjectDecorator)
			def __init__( self, o ): pass
			def decorateExternalObject( self, obj, result ):
				result['test'] = obj

		component.provideSubscriptionAdapter( Decorator, adapts=(ITest,) )

		assert_that( toExternalObject( test ), is_( {'test': test } ) )
