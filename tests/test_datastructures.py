
from hamcrest import (assert_that, is_, has_entry, instance_of,
					  has_key, is_in, not_none, is_not, greater_than,
					  greater_than_or_equal_to, is_in, has_length, has_item,
					  same_instance, only_contains)
from hamcrest.library import has_property as has_attr
import unittest

import UserDict
import collections

import persistent
import json
import plistlib
from nti.dataserver.datastructures import (getPersistentState, toExternalOID, fromExternalOID, toExternalObject,
									   ExternalizableDictionaryMixin, CaseInsensitiveModDateTrackingOOBTree,
									   KeyPreservingCaseInsensitiveModDateTrackingOOBTree,
									   LastModifiedCopyingUserList, PersistentExternalizableWeakList,
									   ContainedStorage, ContainedMixin, CreatedModDateTrackingObject,
									   to_external_representation, EXT_FORMAT_JSON, EXT_FORMAT_PLIST,
									   PersistentExternalizableList, ExternalizableInstanceDict)

from nti.tests import has_attr
import mock_dataserver
from nti.dataserver import contenttypes, ntiids
from nti.dataserver import interfaces as nti_interfaces


class TestFunctions(unittest.TestCase):

	def test_getPersistentState(self):
		# Non-persistent objects are changed
		assert_that( getPersistentState( None ), is_(persistent.CHANGED ) )
		assert_that( getPersistentState( object() ), is_(persistent.CHANGED) )

		# Object with _p_changed are that
		class T(object):
			_p_changed = True

		assert_that( getPersistentState( T() ), is_(persistent.CHANGED) )
		T._p_changed = False
		assert_that( getPersistentState( T() ), is_( persistent.UPTODATE ) )

		# _p_state is trumped by _p_changed
		T._p_state = None
		assert_that( getPersistentState( T() ), is_( persistent.UPTODATE ) )

		# _p_state is used if _p_changed isn't
		del T._p_changed
		T._p_state = 42
		assert_that( getPersistentState( T() ), is_( 42 ) )

		def f(s): return 99
		T.getPersistentState = f
		del T._p_state
		assert_that( getPersistentState( T() ), is_( 99 ) )

	def test_toExternalID( self ):
		class T(object): pass
		assert_that( toExternalOID( T() ), is_(None) )

		t = T()
		t._p_oid = '\x00\x01'
		assert_that( toExternalOID( t ), is_( '0x01' ) )

		t._p_jar = t
		db = T()
		db.database_name = 'foo'
		t.db = lambda: db
		assert_that( toExternalOID( t ), is_( '0x01:666f6f' ) )

		assert_that( fromExternalOID( '0x01:666f6f' )[0], is_( '\x00\x00\x00\x00\x00\00\x00\x01' ) )
		assert_that( fromExternalOID( '0x01:666f6f' )[1], is_( 'foo' ) )


	def test_to_external_representation_none_handling( self ):
		d = {'a': 1, 'None': None}
		# JSON keeps None
		assert_that( json.loads( to_external_representation( d, EXT_FORMAT_JSON ) ),
					 is_( d ) )
		# PList strips it
		assert_that( plistlib.readPlistFromString( to_external_representation( d, EXT_FORMAT_PLIST ) ),
					 is_( { 'a': 1 } ) )

	def test_external_class_name( self ):
		class C(UserDict.UserDict,ExternalizableDictionaryMixin):
			pass
		assert_that( toExternalObject( C() ), has_entry( 'Class', 'C' ) )
		C.__external_class_name__ = 'ExternalC'
		assert_that( toExternalObject( C() ), has_entry( 'Class', 'ExternalC' ) )

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

class TestExternalizableInstanceDict(unittest.TestCase):

	class C(ExternalizableInstanceDict):
		def __init__( self ):
			super(TestExternalizableInstanceDict.C,self).__init__()
			self.A1 = None
			self.A2 = None
			self.A3 = None
			self._A4 = None
			# notice no A5

	def test_simple_roundtrip( self ):
		obj = self.C()
		# Things that are excluded by default
		obj.containerId = 'foo'
		obj.creator = 'foo2'
		obj.id = 'id'

		# Things that should go
		obj.A1 = 1
		obj.A2 = "2"

		# Things that should be excluded dynamically
		def l(): pass
		obj.A3 = l
		obj._A4 = 'A'
		self.A5 = "Not From Init"

		ext = toExternalObject( obj )

		newObj = self.C()
		newObj.updateFromExternalObject( ext )

		for attr in set(obj._excluded_out_ivars_) | set(['A5']):
			assert_that( newObj, does_not( has_attr( attr ) ) )
		assert_that( ext, does_not( has_key( "A5" ) ) )
		assert_that( ext, does_not( has_key( 'A3' ) ) )
		assert_that( ext, does_not( has_key( '_A4' ) ) )
		assert_that( newObj.A1, is_( 1 ) )
		assert_that( newObj.A2, is_( "2" ) )

if __name__ == '__main__':
	unittest.main()
