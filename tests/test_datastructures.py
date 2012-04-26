
from hamcrest import (assert_that, is_, has_entry, instance_of )
from hamcrest import  is_in, not_none, is_not, greater_than
from hamcrest import greater_than_or_equal_to,  has_item
from hamcrest import same_instance
from hamcrest.library import has_property as has_attr
import unittest


import collections

import persistent

from nti.dataserver.datastructures import ModDateTrackingObject, ModDateTrackingOOBTree
from nti.dataserver.datastructures import CaseInsensitiveModDateTrackingOOBTree
from nti.dataserver.datastructures import KeyPreservingCaseInsensitiveModDateTrackingOOBTree
from nti.dataserver.datastructures import LastModifiedCopyingUserList
from nti.dataserver.datastructures import ContainedStorage, ContainedMixin, CreatedModDateTrackingObject

from nti.externalization.externalization import toExternalObject
from nti.externalization.oids import toExternalOID


from nti.tests import has_attr
import mock_dataserver
from nti.dataserver import contenttypes
from nti.dataserver import interfaces as nti_interfaces
from nti.ntiids import ntiids

def test_moddatetrackingobject_oldstates():
	mto = ModDateTrackingObject()
	assert_that( mto.lastModified, is_( 0 ) )
	assert_that( mto._lastModified, has_attr( 'value', 0 ) )

	# old state
	mto._lastModified = 32
	assert_that( mto.lastModified, is_( 32 ) )
	assert_that( mto._lastModified, is_( 32 ) )

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

def test_moddatetrackingoobtree_resolveConflict():
	mto = ModDateTrackingOOBTree()
	mto['k'] = 'v'
	oldstate = mto.__getstate__()

	mto.updateLastMod( 8 )
	savedstate = mto.__getstate__()

	mto.updateLastMod( 10 )
	newstate = mto.__getstate__()

	# Make sure it runs w/o exception
	# TODO: How to ensure it does the right thing? We don't know the times
	mto._p_resolveConflict( oldstate, savedstate, newstate )


class TestCaseInsensitiveModDateTrackingOOBTree(unittest.TestCase):

	def test_get_set_contains( self ):
		assert_that( CaseInsensitiveModDateTrackingOOBTree(), instance_of( collections.Mapping ) )
		assert_that( 'k', is_in( CaseInsensitiveModDateTrackingOOBTree( {'K': 1 } ) ) )
		assert_that( 'K', is_in( CaseInsensitiveModDateTrackingOOBTree( {'K': 1 } ) ) )

		# The values are lowercased
		cap_k = CaseInsensitiveModDateTrackingOOBTree( {'K': 1 } )
		assert_that( 'k', is_in( list(cap_k.keys()) ) )
		assert_that( ('k', 1), is_in( cap_k.items() ) )

class TestKeyPreservingCaseInsensitiveModDateTrackingOOBTree(unittest.TestCase):

	def test_get_set_contains( self ):
		assert_that( KeyPreservingCaseInsensitiveModDateTrackingOOBTree(), instance_of( collections.Mapping ) )
		assert_that( 'k', is_in( KeyPreservingCaseInsensitiveModDateTrackingOOBTree( {'K': 1 } ) ) )
		assert_that( 'K', is_in( KeyPreservingCaseInsensitiveModDateTrackingOOBTree( {'K': 1 } ) ) )

		# The key values remain the same
		cap_k = KeyPreservingCaseInsensitiveModDateTrackingOOBTree( {'K': 1 } )
		assert_that( 'K', is_in( list(cap_k.keys()) ) )
		assert_that( ('K', 1), is_in( cap_k.items() ) )

		assert_that( 'K', is_in( list(cap_k.iterkeys()) ) )
		assert_that( ('K', 1), is_in( list( cap_k.iteritems() ) ) )

		# iterating
		assert_that( 'K', is_in( [k for k in cap_k] ) )
		assert_that( [k for k in cap_k], has_item( 'K' ) )

		# Magic key
		assert_that( 'Last Modified', is_in( cap_k.keys() ) )
		assert_that( cap_k, has_entry( 'Last Modified', greater_than_or_equal_to( 1 ) ) )
		assert_that( cap_k['Last Modified'], greater_than_or_equal_to( 1 ) )

		# The key can be deleted
		del cap_k['k']
		assert_that( 'K', is_not(is_in( cap_k ) ))

	def test_copy( self ):
		low_k = CaseInsensitiveModDateTrackingOOBTree( {'K': 1 } )
		cap_k = KeyPreservingCaseInsensitiveModDateTrackingOOBTree( low_k )
		assert_that( 'k', is_in( cap_k ) )
		assert_that( 'K', is_in( cap_k ) )

class TestLastModifiedCopyingUserList(unittest.TestCase):

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
class TestPersistentExternalizableWeakList(unittest.TestCase):

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

class TestContainedStorage(mock_dataserver.ConfiguringTestBase):

	class C(CreatedModDateTrackingObject,ContainedMixin): pass

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
		obj.containerId = 'foo'
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
from .mock_dataserver import ConfiguringTestBase
from nti.externalization.interfaces import IExternalObject, IExternalObjectDecorator


class TestToExternalObject(ConfiguringTestBase):

	def test_decorator(self):
		class ITest(interface.Interface): pass
		class Test(object):
			interface.implements(ITest,IExternalObject)

			def toExternalObject(self):
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

if __name__ == '__main__':
	unittest.main()
