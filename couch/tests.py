import json
import re

import couch

import tornado.ioloop

def run_blocking_tests():
    # set up tests
    doc1 = {'msg': 'Test doc 1'}
    doc2 = {'msg': 'Test doc 2'}

    db = couch.BlockingCouch('testdb')
    db2 = couch.BlockingCouch('testdb2')

    try:
        db.delete_db()
    except couch.NotFound:
        pass
    try:
        db2.delete_db()
    except couch.NotFound:
        pass

    # create database
    resp = db.create_db()
    assert 'ok' in resp, 'Failed to create database'

    # list databases
    resp = db.list_dbs()
    assert db.db_name in resp, 'Database not in list of databases'

    # info_db
    resp = db.info_db()
    assert ('db_name' in resp) and (resp['db_name'] == db.db_name), 'No database info'

    # uuids
    resp = db.uuids()
    assert re.match('[0-9a-f]{32}', resp[0]), 'Failed to get uuid'

    # save doc
    resp = db.save_doc(doc1)
    assert ('rev' in resp) and ('id' in resp), 'Failed to save doc'
    doc1.update({'_id':resp['id'], '_rev':resp['rev']})

    # save doc with wrong rev number
    try:
        resp = db.save_doc({'_id': doc1['_id'], '_rev': 'a'})
    except couch.CouchException:
        pass
    else:
        raise AssertionError('No error when overwriting doc with wrong rev')

    # get doc
    resp = db.get_doc(doc1['_id'])
    assert doc1 == resp, 'Failed to get doc'

    # get non-existing doc
    try:
        resp = db.get_doc('a')
    except couch.NotFound:
        pass
    else:
        raise AssertionError('No error on request for unexisting doc')

    # save docs
    doc1['msg2'] = 'Another message'
    resp = db.save_docs([doc1, doc2])
    assert all('rev' in item and 'id' in item for item in resp), 'Failed to save docs'
    doc1['_rev'] = resp[0]['rev']
    doc2.update({'_id': resp[1]['id'], '_rev': resp[1]['rev']})

    # get docs
    resp = db.get_docs([doc1['_id'], doc2['_id']])
    assert [doc1, doc2] == resp, 'Failed to get docs'

    # get non-existing docs
    try:
        resp = db.get_docs(['a', 'b'])
    except couch.NotFound:
        pass
    else:
        raise AssertionError('No error on request for unexisting docs')

    # list docs
    resp = db.view_all_docs(include_docs=True)
    assert {doc1['_id']:doc1['_rev'], doc2['_id']:doc2['_rev']} == dict((row['doc']['_id'], row['doc']['_rev']) for row in resp['rows']), 'Failed listing all docs'

    # pull database
    resp = db2.pull_db('testdb', create_target=True)
    assert 'ok' in resp, 'Replication failed'
    assert 'testdb2' in db2.list_dbs(), 'Replication failed, new database replication not found'

    # delete docs
    resp = db2.delete_docs([doc1, doc2])
    assert resp[0]['id']==doc1['_id'] and resp[1]['id']==doc2['_id'], 'Failed to delete docs'
    assert len(db2.view_all_docs()['rows'])==0, 'Failed to delete docs, database not empty'

    # delete database
    resp = db2.delete_db()
    assert 'ok' in resp, 'Failed to delete database'

    # upload design doc
    design = {
        '_id': '_design/test',
        'views': {
            'msg': {
                'map': 'function(doc) { if (doc.msg) { emit(doc._id, doc.msg); } }'
            }
        }
    }
    resp = db.save_doc(design)
    assert 'ok' in resp, 'Failed to upload design doc'
    design['_rev'] = resp['rev']

    # view
    resp = db.view('test', 'msg')
    assert [doc1['_id'], doc2['_id']] == [row['key'] for row in resp['rows']], 'Failed to get view results from design doc'

    # delete doc
    resp = db.delete_doc(doc2)
    assert resp['id'] == doc2['_id'], 'Failed to delete doc2'

    # save attachment
    data = {'msg3': 'This is a test'}
    attachment = {'mimetype': 'application/json', 'name': 'test attachment', 'data': json.dumps(data)}
    resp = db.save_attachment(doc1, attachment)
    assert 'ok' in resp, 'Attachment not saved'
    doc1['_rev'] = resp['rev']

    # get attachment
    resp = db.get_attachment(doc1, attachment['name'], attachment['mimetype'])
    assert json.loads(resp) == data, 'Attachment not loaded'

    # delete attachment
    resp = db.delete_attachment(doc1, attachment['name'])
    assert 'ok' in resp, 'Attachment not deleted'
    doc1['_rev'] = resp['rev']

    db.delete_db()

    print('All blocking tests passed')


class AsyncTests(object):

    def __init__(self):
        # set up tests
        self.doc1 = {'msg': 'Test doc 1'}
        self.doc2 = {'msg': 'Test doc 2'}

        self.db = couch.AsyncCouch('testdb')
        self.db2 = couch.AsyncCouch('testdb2')

        self.db.delete_db(self.init_deleted_db)

        self.error = None
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.ioloop.start()

        if self.error:
            raise AssertionError(self.error)
        else:
            print('All async tests passed')

    def check(self, value, msg):
        if not value:
            self.error = msg
            self.ioloop.stop()
            return False
        return True

    def init_deleted_db(self, resp):
        self.db2.delete_db(self.init_deleted_db2)

    def init_deleted_db2(self, resp):
        # create database
        self.db.create_db(self.created_db)

    def created_db(self, resp):
        if self.check('ok' in resp, 'Failed to create database'):
            # list databases
            self.db.list_dbs(self.listed_dbs)

    def listed_dbs(self, resp):
        if self.check(self.db.db_name in resp,
                'Database not in list of databases'):
            # info_db
            self.db.info_db(self.info_db)

    def info_db(self, resp):
        if self.check(('db_name' in resp) and
                (resp['db_name'] == self.db.db_name), 'No database info'):
            # uuids
            self.db.uuids(callback=self.uuids)

    def uuids(self, resp):
        if self.check(re.match('[0-9a-f]{32}', resp[0]), 'Failed to get uuid'):
            # save doc
            self.db.save_doc(self.doc1, self.saved_doc1)

    def saved_doc1(self, resp):
        if self.check(('rev' in resp) and ('id' in resp), 'Failed to save doc'):
            self.doc1.update({'_id':resp['id'], '_rev':resp['rev']})

            # save doc with wrong rev number
            self.db.save_doc({'_id': self.doc1['_id'], '_rev': 'a'},
                    self.saved_doc1_norev)

    def saved_doc1_norev(self, resp):
        if self.check(isinstance(resp, couch.CouchException),
                'No error when overwriting doc with wrong rev'):
            # get doc
            self.db.get_doc(self.doc1['_id'], self.got_doc1)

    def got_doc1(self, resp):
        if self.check(self.doc1 == resp, 'Failed to get doc'):
            # get non-existing doc
            self.db.get_doc('a', self.got_nodoc)

    def got_nodoc(self, resp):
        if self.check(isinstance(resp, couch.NotFound),
                'No error on request for unexisting doc'):
            # save docs
            self.doc1['msg2'] = 'Another message'
            self.db.save_docs([self.doc1, self.doc2], self.saved_docs)

    def saved_docs(self, resp):
        if self.check(all('rev' in item and 'id' in item for item in resp),
                'Failed to save docs'):
            self.doc1['_rev'] = resp[0]['rev']
            self.doc2.update({'_id': resp[1]['id'], '_rev': resp[1]['rev']})

            # get docs
            self.db.get_docs([self.doc1['_id'], self.doc2['_id']],
                    self.got_docs)

    def got_docs(self, resp):
        if self.check([self.doc1, self.doc2] == resp, 'Failed to get docs'):
            # get non-existing docs
            self.db.get_docs(['a', 'b'], self.got_nodocs)

    def got_nodocs(self, resp):
        if self.check(isinstance(resp, couch.NotFound),
                'No error on request for unexisting docs'):
            # list docs
            self.db.view_all_docs(self.list_docs, include_docs=True)

    def list_docs(self, resp):
        if self.check({self.doc1['_id']: self.doc1['_rev'],
                self.doc2['_id']: self.doc2['_rev']} ==
                dict((row['doc']['_id'], row['doc']['_rev'])
                for row in resp['rows']), 'Failed listing all docs'):
            # pull database
            self.db2.pull_db('testdb', self.pulled_db, create_target=True)

    def pulled_db(self, resp):
        if self.check('ok' in resp, 'Replication failed'):
           self.db2.list_dbs(self.pulled_db_verified)

    def pulled_db_verified(self, resp):
        if self.check('testdb2' in resp, 'Replication failed, new database '
                'replication not found'):
            # delete docs
            self.db2.delete_docs([self.doc1, self.doc2], self.deleted_docs)

    def deleted_docs(self, resp):
        if self.check(resp[0]['id']==self.doc1['_id'] and
                resp[1]['id']==self.doc2['_id'], 'Failed to delete docs'):
            self.db2.view_all_docs(self.deleted_docs_verified)

    def deleted_docs_verified(self, resp):
        if self.check(len(resp['rows'])==0, 'Failed to delete docs, database not empty'):
            # delete database
            self.db2.delete_db(self.deleted_db2)

    def deleted_db2(self, resp):
        if self.check('ok' in resp, 'Failed to delete database'):
            # upload design doc
            self.design = {
                '_id': '_design/test',
                'views': {
                    'msg': {
                        'map': 'function(doc) { if (doc.msg) { emit(doc._id, doc.msg); } }'
                    }
                }
            }
            self.db.save_doc(self.design, self.saved_design)

    def saved_design(self, resp):
        if self.check('ok' in resp, 'Failed to upload design doc'):
            self.design['_rev'] = resp['rev']

            # view
            self.db.view('test', 'msg', self.viewed)

    def viewed(self, resp):
        if self.check([self.doc1['_id'], self.doc2['_id']] ==
                [row['key'] for row in resp['rows']],
                'Failed to get view results from design doc'):
            # delete doc
            self.db.delete_doc(self.doc2, self.deleted_doc2)

    def deleted_doc2(self, resp):
        if self.check(resp['id'] == self.doc2['_id'], 'Failed to delete doc2'):
            # save attachment
            self.data = {'msg3': 'This is a test'}
            self.attachment = {'mimetype': 'application/json',
                    'name': 'test attachment', 'data': json.dumps(self.data)}

            self.db.save_attachment(self.doc1, self.attachment,
                    self.saved_attachment)

    def saved_attachment(self, resp):
        if self.check('ok' in resp, 'Attachment not saved'):
            self.doc1['_rev'] = resp['rev']

            # get attachment
            self.db.get_attachment(self.doc1, self.attachment['name'],
                self.attachment['mimetype'], callback=self.got_attachment)

    def got_attachment(self, resp):
        if self.check(json.loads(resp) == self.data, 'Attachment not loaded'):
            # delete attachment
            self.db.delete_attachment(self.doc1, self.attachment['name'],
                self.deleted_attachment)

    def deleted_attachment(self, resp):
        if self.check('ok' in resp, 'Attachment not deleted'):
            self.doc1['_rev'] = resp['rev']

            # done testing
            cb = lambda r: self.ioloop.stop()
            self.db.delete_db(cb)


if __name__ == '__main__':
    run_blocking_tests()
    AsyncTests()
