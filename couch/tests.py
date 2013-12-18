import json
import re

import couch

from tornado import ioloop, gen


dbname1 = 'tornado-couch-testdb'
dbname2 = 'tornado-couch-testdb2'


def run_blocking_tests():
    # set up tests
    doc1 = {'msg': 'Test doc 1'}
    doc2 = {'msg': 'Test doc 2'}

    db = couch.BlockingCouch(dbname1)
    db2 = couch.BlockingCouch(dbname2)

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
    assert ('db_name' in resp) and (resp['db_name'] == db.db_name), \
        'No database info'

    # uuids
    resp = db.uuids()
    assert re.match('[0-9a-f]{32}', resp[0]), 'Failed to get uuid'

    # save doc
    resp = db.save_doc(doc1)
    assert ('rev' in resp) and ('id' in resp), 'Failed to save doc'
    doc1.update({'_id': resp['id'], '_rev': resp['rev']})

    # save doc with wrong rev number
    try:
        db.save_doc({'_id': doc1['_id'], '_rev': 'a'})
        raise AssertionError('No error when overwriting doc with wrong rev')
    except couch.CouchException:
        pass

    # has doc
    resp = db.has_doc(doc1['_id'])
    assert resp, "Failed on getting head of doc"

    # has doc on non-existing doc
    resp = db.has_doc('a')
    assert not resp, "Failed on getting head of non-existing doc"

    # get doc
    resp = db.get_doc(doc1['_id'])
    assert doc1 == resp, 'Failed to get doc'

    # get non-existing doc
    try:
        resp = db.get_doc('a')
        raise AssertionError('No error on request for unexisting doc')
    except couch.NotFound:
        pass

    # save docs
    doc1['msg2'] = 'Another message'
    resp = db.save_docs([doc1, doc2])
    assert all('rev' in item and 'id' in item for item in resp), \
        'Failed to save docs'
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
    assert {doc1['_id']: doc1['_rev'], doc2['_id']: doc2['_rev']} == \
        dict((row['doc']['_id'], row['doc']['_rev'])
             for row in resp['rows']), 'Failed listing all docs'

    # pull database
    resp = db2.pull_db(dbname1, create_target=True)
    assert 'ok' in resp, 'Replication failed'
    assert dbname2 in db2.list_dbs(), \
        'Replication failed, new database replication not found'

    # delete docs
    resp = db2.delete_docs([doc1, doc2])
    assert resp[0]['id'] == doc1['_id'] and resp[1]['id'] == doc2['_id'], \
        'Failed to delete docs'
    assert len(db2.view_all_docs()['rows']) == 0, \
        'Failed to delete docs, database not empty'

    # delete database
    resp = db2.delete_db()
    assert 'ok' in resp, 'Failed to delete database'

    # upload design doc
    design = {
        '_id': '_design/test',
        'views': {
            'msg': {
                'map': 'function(doc) { if (doc.msg) { '
                       'emit(doc._id, doc.msg); } }'
            }
        }
    }
    resp = db.save_doc(design)
    assert 'ok' in resp, 'Failed to upload design doc'
    design['_rev'] = resp['rev']

    # view
    resp = db.view('test', 'msg')
    assert [doc1['_id'], doc2['_id']] == \
        [row['key'] for row in resp['rows']], \
        'Failed to get view results from design doc'

    # delete doc
    resp = db.delete_doc(doc2)
    assert resp['id'] == doc2['_id'], 'Failed to delete doc2'

    # save attachment
    data = {'msg3': 'This is a test'}
    attachment = {'mimetype': 'application/json', 'name': 'test attachment',
                  'data': json.dumps(data)}
    resp = db.save_attachment(doc1, attachment)
    assert 'ok' in resp, 'Attachment not saved'
    doc1['_rev'] = resp['rev']

    # get attachment
    resp = db.get_attachment(doc1, attachment['name'], attachment['mimetype'])
    assert json.loads(resp.decode('utf8')) == data, 'Attachment not loaded'

    # delete attachment
    resp = db.delete_attachment(doc1, attachment['name'])
    assert 'ok' in resp, 'Attachment not deleted'
    doc1['_rev'] = resp['rev']

    db.delete_db()

    print('All blocking tests passed')


@gen.coroutine
def run_async_tests():

    # set up tests
    doc1 = {'msg': 'Test doc 1'}
    doc2 = {'msg': 'Test doc 2'}

    db = couch.AsyncCouch(dbname1)
    db2 = couch.AsyncCouch(dbname2)

    try:
        yield db.delete_db()
    except couch.NotFound:
        pass
    try:
        yield db2.delete_db()
    except couch.NotFound:
        pass

    # create database
    resp = yield db.create_db()
    assert 'ok' in resp, 'Failed to create database'

    # list databases
    resp = yield db.list_dbs()
    assert db.db_name in resp, 'Database not in list of databases'

    # info_db
    resp = yield db.info_db()
    assert ('db_name' in resp) and (resp['db_name'] == db.db_name), \
        'No database info'

    # uuids
    resp = yield db.uuids()
    assert re.match('[0-9a-f]{32}', resp[0]), 'Failed to get uuid'

    # save doc
    resp = yield db.save_doc(doc1)
    assert ('rev' in resp) and ('id' in resp), 'Failed to save doc'
    doc1.update({'_id': resp['id'], '_rev': resp['rev']})

    # save doc with wrong rev number
    try:
        yield db.save_doc({'_id': doc1['_id'], '_rev': 'a'})
        raise AssertionError('No error when overwriting doc with wrong rev')
    except couch.CouchException:
        pass

    # get doc
    resp = yield db.get_doc(doc1['_id'])
    assert doc1 == resp, 'Failed to get doc'

    # get non-existing doc
    try:
        yield db.get_doc('a')
        raise AssertionError('No error on request for unexisting doc')
    except couch.NotFound:
        pass

    # has doc
    resp = yield db.has_doc(doc1['_id'])
    assert resp, "Failed to get doc HEAD"

    # has doc on non-existing doc
    resp = yield db.has_doc('a')
    assert not resp, "Has a non-existing doc"

    # save docs
    doc1['msg2'] = 'Another message'
    resp = yield db.save_docs([doc1, doc2])
    assert all('rev' in item and 'id' in item for item in resp), \
        'Failed to save docs'
    doc1['_rev'] = resp[0]['rev']
    doc2.update({'_id': resp[1]['id'], '_rev': resp[1]['rev']})

    # get docs
    resp = yield db.get_docs([doc1['_id'], doc2['_id']])
    assert [doc1, doc2] == resp, 'Failed to get docs'

    # get non-existing docs
    try:
        yield db.get_docs(['a', 'b'])
        raise AssertionError('No error on request for unexisting docs')
    except couch.NotFound:
        pass

    # list docs
    resp = yield db.view_all_docs(include_docs=True)
    assert {doc1['_id']: doc1['_rev'], doc2['_id']: doc2['_rev']} == \
        dict((row['doc']['_id'], row['doc']['_rev'])
             for row in resp['rows']), 'Failed listing all docs'

    # pull database
    resp = yield db2.pull_db(dbname1, create_target=True)
    assert 'ok' in resp, 'Replication failed'

    # verify that replicated db is in the list of dbs
    resp = yield db2.list_dbs()
    assert dbname2 in resp, \
        'Replication failed, new database replication not found'

    # delete docs
    resp = yield db2.delete_docs([doc1, doc2])
    assert resp[0]['id'] == doc1['_id'] and \
        resp[1]['id'] == doc2['_id'], 'Failed to delete docs'

    # check that deleted docs are not in the list all docs
    resp = yield db2.view_all_docs()
    assert len(resp['rows']) == 0, 'Failed to delete docs, database not empty'

    # delete database
    resp = yield db2.delete_db()
    assert 'ok' in resp, 'Failed to delete database'

    # upload design doc
    design = {
        '_id': '_design/test',
        'views': {
            'msg': {
                'map': 'function(doc) { if (doc.msg) { '
                       'emit(doc._id, doc.msg); } }'
            }
        }
    }
    resp = yield db.save_doc(design)
    assert 'ok' in resp, 'Failed to upload design doc'
    design['_rev'] = resp['rev']

    # view
    resp = yield db.view('test', 'msg')
    assert [doc1['_id'], doc2['_id']] == \
        [row['key'] for row in resp['rows']], \
        'Failed to get view results from design doc'

    # delete doc
    resp = yield db.delete_doc(doc2)
    assert resp['id'] == doc2['_id'], 'Failed to delete doc2'

    # save attachment
    data = {'msg3': 'This is a test'}
    attachment = {'mimetype': 'application/json',
                  'name': 'test attachment', 'data': json.dumps(data)}

    resp = yield db.save_attachment(doc1, attachment)
    assert 'ok' in resp, 'Attachment not saved'
    doc1['_rev'] = resp['rev']

    # get attachment
    resp = yield db.get_attachment(doc1, attachment['name'],
                                   attachment['mimetype'])
    assert json.loads(resp.decode('utf8')) == data, \
        'Attachment not loaded'

    # delete attachment
    resp = yield db.delete_attachment(doc1, attachment['name'])
    assert 'ok' in resp, 'Attachment not deleted'
    doc1['_rev'] = resp['rev']

    # done testing, delete test db
    yield db.delete_db()

    print('All async tests passed')


if __name__ == '__main__':
    run_blocking_tests()
    ioloop.IOLoop.instance().run_sync(run_async_tests)
