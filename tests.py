import json
import re

import couch

def run_blocking_tests():
    # set up tests
    doc1 = {'msg': 'Test doc 1'}
    doc2 = {'msg': 'Test doc 2'}

    db = couch.BlockingCouch('testdb')
    db2 = couch.BlockingCouch('testdb2')

    db.delete_db(raise_error=False)
    db2.delete_db(raise_error=False)

    # create database
    resp = db.create_db()
    assert resp == {u'ok': True}, 'Failed to create database'

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
    resp = db.save_doc({'_id': doc1['_id'], '_rev': 'a'}, raise_error=False)
    assert 'error' in resp, 'No error when overwriting doc with wrong rev'

    # get doc
    resp = db.get_doc(doc1['_id'])
    assert doc1 == resp, 'Failed to get doc'
    resp = db.get_doc('a', raise_error=False)
    assert 'error' in resp, 'No error on request for unexisting doc'

    # save docs
    doc1['msg2'] = 'Another message'
    resp = db.save_docs([doc1, doc2])
    assert all('rev' in item and 'id' in item for item in resp), 'Failed to save docs'
    doc1['_rev'] = resp[0]['rev']
    doc2.update({'_id':resp[1]['id'], '_rev':resp[1]['rev']})

    # get docs
    resp = db.get_docs([doc1['_id'], doc2['_id']])
    assert [doc1, doc2] == resp, 'Failed to get docs'
    resp = db.get_docs(['a'], raise_error=False)
    assert 'error' in resp[0], 'No error on request for unexisting doc'
   
    # list docs
    resp = db.list_docs()
    assert {doc1['_id']:doc1['_rev'], doc2['_id']:doc2['_rev']} == resp, 'Failed listing all docs'

    # pull database
    resp = db2.pull_db('testdb', create_target=True)
    assert 'ok' in resp, 'Replication failed'
    assert 'testdb2' in db2.list_dbs(), 'Replication failed, new database replication not found'

    # delete docs
    resp = db2.delete_docs([doc1, doc2])
    assert resp[0]['id']==doc1['_id'] and resp[1]['id']==doc2['_id'], 'Failed to delete docs'
    assert not db2.list_docs(), 'Failed to delete docs, database not empty'

    # delete database
    resp = db2.delete_db()
    assert resp == {u'ok': True}

    # view (and first upload design doc)
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
    resp = db.view('test', 'msg')
    assert [doc1['_id'], doc2['_id']] == [row['key'] for row in resp['rows']], 'Failed to get view results from design doc'

    # delete doc
    resp = db.delete_doc(doc2)
    assert resp['id'] == doc2['_id']

    # save attachment
    data = {'msg3': 'This is a test'}
    attachment = {'mimetype': 'application/json', 'name': 'test attachment', 'data': json.dumps(data)}
    resp = db.save_attachment(doc1, attachment)
    assert 'ok' in resp, 'Attachment not saved'
    doc1['_rev'] = resp['rev']

    # get attachment
    resp = db.get_attachment(doc1, 'test attachment', attachment['mimetype'])
    assert json.loads(resp) == data, 'Attachment not loaded'

    # delete attachment
    resp = db.delete_attachment(doc1, 'test attachment')
    assert 'ok' in resp, 'Attachment not deleted'
    doc1['_rev'] = resp['rev']
    
    db.delete_db()

if __name__ == '__main__':
    run_blocking_tests()

