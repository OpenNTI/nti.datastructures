#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods
# pylint: disable=global-statement,broad-except
# pylint: disable=arguments-differ,no-member

import functools

import transaction

import ZODB

from ZODB.DemoStorage import DemoStorage

import zope.testing.cleanup

from zope import component

from zope.component.hooks import setHooks
from zope.component.hooks import site as currentSite

from zope.site import LocalSiteManager
from zope.site import SiteManagerContainer

from zope.site.folder import rootFolder

from nti.testing.layers import GCLayerMixin
from nti.testing.layers import ZopeComponentLayer
from nti.testing.layers import ConfiguringLayerMixin

current_mock_db = None
current_transaction = None
root_name = 'nti.dataserver'
 
 
def install_main(conn):
    root = conn.root()
    root_folder = rootFolder()
    conn.add(root_folder)
    root_sm = LocalSiteManager(root_folder)
    conn.add(root_sm)
    root_folder.setSiteManager(root_sm)
    root[root_name] = root_folder
 
 
def init_db(db, conn=None):
    conn = db.open() if conn is None else conn
    global current_transaction
    if current_transaction != conn:
        current_transaction = conn
    install_main(conn)
    return conn
 
 
class mock_db_trans(object):
 
    def __init__(self, db=None):
        self.conn = None
        self._site_cm = None
        self.db = db or current_mock_db
 
    def _check(self, conn):
        root = conn.root()
        if root_name not in root:
            install_main(conn)
 
    def __enter__(self):
        transaction.begin()
        self.conn = conn = self.db.open()
        global current_transaction
        current_transaction = conn
        self._check(conn)
        sitemanc = conn.root()[root_name]
        self._site_cm = currentSite(sitemanc)
        self._site_cm.__enter__()
        return conn
 
    def __exit__(self, t, v, tb):
        result = self._site_cm.__exit__(t, v, tb)
        global current_transaction
        body_raised = t is not None
        try:
            try:
                if not transaction.isDoomed():
                    transaction.commit()
                else:  # pragma: no cover
                    transaction.abort()
            except Exception:  # pragma: no cover
                transaction.abort()
                raise
            finally:
                current_transaction = None
                self.conn.close()
        except Exception:  # pragma: no cover
            if not body_raised:
                raise
        reset_db_caches(self.db)
        return result
 
 
def reset_db_caches(db=None):
    if db is not None:
        db.pool.map(lambda conn: conn.cacheMinimize())
 
 
def _mock_ds_wrapper_for(func, db, teardown=None):
 
    @functools.wraps(func)
    def f(*args):
        global current_mock_db
        current_mock_db = db
        init_db(db)
 
        sitemanc = SiteManagerContainer()
        sitemanc.setSiteManager(LocalSiteManager(None))
 
        with currentSite(sitemanc):
            assert component.getSiteManager() == sitemanc.getSiteManager()
            try:
                func(*args)
            finally:
                current_mock_db = None
                if teardown:
                    teardown()
    return f
 
 
def WithMockDS(*args):
    def teardown():
        return None
    db = ZODB.DB(DemoStorage(name='Users'))
    func = args[0]
    return _mock_ds_wrapper_for(func, db, teardown)


class SharedConfiguringTestLayer(ZopeComponentLayer,
                                 GCLayerMixin,
                                 ConfiguringLayerMixin):

    set_up_packages = ('nti.datastructures',)

    @classmethod
    def setUp(cls):
        cls.setUpPackages()

    @classmethod
    def tearDown(cls):
        cls.tearDownPackages()
        zope.testing.cleanup.cleanUp()

    @classmethod
    def testSetUp(cls, test=None):
        pass

    @classmethod
    def testTearDown(cls):
        pass
