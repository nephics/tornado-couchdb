# Blocking and non-blocking (asynchronous) clients for CouchDB using Tornado's httpclient

This Python module wraps the CouchDB HTTP REST API and defines a common interface for making blocking and non-blocking operations on a CouchDB.

## Install

Install using `pip`:

    pip install https://bitbucket.org/nephics/tornado-couchdb/get/default.zip

The code has been tested with Python 3.3 and 2.7.

## BlockingCouch

The BlockingCouch class is a basic wrapper for making blocking operations on a CouchDB. Using this class implies that the Tornado eventloop is blocked on a database call, waiting for reply from the database.

Use this class when blocking the eventloop *is not* a problem, or when there is a *low latency connection* to the database, and the operations are "small", i.e., only takes short time (in the range of tens of miliseconds) to complete.

Example usage:

    import couch
    
    db = couch.BlockingCouch('mytestdb')
    db.create_db()
    r = db.save_doc({'msg': 'My first document'})
    doc = db.get_doc(r['id'])
    db.delete_doc(doc)

For any methods of this class: If an error is returned from the database,
an exception is raised using an appropriate sub-class of CouchException.

Example error handling:

    import couch

    db = couch.AsyncCouch('mytestdb')
    try:
        doc = db.get_doc('non-existing-id')
    except couch.NotFound:
        print('Document not found')


## AsyncCouch

The AsyncCouch class is a basic wrapper for making non-blocking operations on a CouchDB. Using this class implies that the Tornado eventloop is *not* blocked on  a database call, and reply from the database is returned as a Future or delivered to a callback function.

Use this class when blocking the eventloop *is* a problem, or when there is a *high latency connection* to the database, and the operations are "large", i.e., takes long time (in the range of seconds) to complete.

Example usage with coroutine:

    import couch
    from tornado import ioloop, gen

    @gen.coroutine
    def run_test():
        db = couch.AsyncCouch('mytestdb')
        yield db.create_db()
        r = yield db.save_doc({'msg': 'My first document'})
        doc = yield db.get_doc(r['id'])
        yield db.delete_doc(doc)
        yield db.delete_db()

    ioloop.IOLoop.run_sync(run_test)

For any methods of this class: If the database call results in an error,
an exception is raised at the callpoint using an appropriate sub-class of CouchException. This applies *both* when calling with a callback function and when yielding to a Future (using gen.coroutine).

Example error handling:

    import couch
    from tornado import ioloop, gen
     
    @gen.coroutine
    def main():
         db = couch.AsyncCouch('mytestdb')
         try:
             doc = yield db.get_doc('non-existing-id')
         except couch.NotFound:
             print('Document not found')
     
     ioloop.IOLoop.run_sync(main)
 
Note: In versions before 0.2.0, if error occured in the database call,  AsyncCouch would pass the exception as a parameter to the callback function.


## Database methods

Database related methods.

    create_db(self, db_name=None):
        Creates a new database.
    
    delete_db(self, db_name=None):
        Deletes the database.

    list_dbs(self):
        List names of databases.

    info_db(self, db_name=None):
        Get info about the database.

    pull_db(self, source, db_name=None, create_target=False):
        Replicate changes from a source database to current (target)
        database.

    uuids(self, count=1):
        Get one or more uuids.

Document related methods.

    get_doc(self, doc_id):
        Get document with the given `doc_id`.
    
    get_docs(self, doc_ids):
        Get multiple documents with the given list of `doc_ids`.
        
        Response is a list with the requested documents, in same order as the
        provided document id's.
        
        If one or more documents are not found in the database, a NotFound
        exception is raised.

    has_doc(self, doc_id):
        See whether given `doc_id` existed in then given database.
        Return True/False

    save_doc(self, doc):
        Save/create a document to/in a given database. Response is a dict
        with id and rev of the saved doc.
    
    save_docs(self, docs, all_or_nothing=False):
        Save/create multiple documents.
        Response is a list of dicts with id and rev of the saved docs.

    delete_doc(self, doc):
        Delete a document
    
    delete_docs(self, docs, all_or_nothing=False):
        Delete multiple documents

    get_attachment(self, doc, attachment_name, mimetype=None):
        Get document attachment.
        The parameter `doc` should at least contain an `_id` key.
        If mimetype is not specified, `doc` shall contain an `_attachments`
        key with info about the named attachment.

    save_attachment(self, doc, attachment):
        Save an attachment to the specified doc.
        The attachment shall be a dict with keys: `mimetype`, `name`, `data`.
        The doc shall be a dict, at least having the key `_id`, and if doc is
        existing in the database, it shall also contain the key `_rev`

    delete_attachment(self, doc, attachment_name):
        Delete a named attachment to the specified doc.
        The doc shall be a dict, at least with the keys: _id and _rev
    
    view(self, design_doc_name, view_name, **kwargs):
        Query a pre-defined view in the specified design doc.
        The following query parameters can be specified as keyword arguments.
        
        Limit query results to those with the specified key or list of keys:
          key=<key-value>
          keys=<list of keys>
        
        Limit query results to those following the specified startkey:
          startkey=<key-value>
        
        First document id to include in the output:
          startkey_docid=<document id>
        
        Limit query results to those previous to the specified endkey:
          endkey=<key-value>
        
        Last document id to include in the output:
          endkey_docid=<document id>
        
        Limit the number of documents in the output:
          limit=<number of docs>
        
        Prevent CouchDB from refreshing a stale view:
          stale='ok'
          stale='update_after'
        
        Reverse the output:
          descending=true
          descending=false  (default value)
        
        Note that the descending option is applied before any key filtering, so
        you may need to swap the values of the startkey and endkey options to
        get the expected results.
        
        Skip the specified number of docs in the query results:
          skip=<number>  (default value is 0)
        
        The group option controls whether the reduce function reduces to a set
        of distinct keys or to a single result row:
          group=true
          group=false  (default value)
        
          group_level=<number>
        
        Use the reduce function of the view:
          reduce=true  (default value)
          reduce=false
        
        Note that default value of reduce is true, only if a reduce function is
        defined for the view.
        
        Automatically fetch and include the document which emitted each view
        entry:
          include_docs=true
          include_docs=false  (default value)
        
        Determine whether the endkey is included in the result:
          inclusive_end=true  (default value)
          inclusive_end=false
    
    view_all_docs(self, **kwargs):
        Query the _all_docs view.
        Accepts the same keyword parameters as `view()`.

    temp_view(self, view_doc, **kwargs):
        Query a temporary view.
        The view_doc parameter is a dict with the view's map and reduce
        functions.


## Exceptions on database call errors

The following exception classes are used for the various database call errors:

`CouchException`: Base class for CouchDB specific exceptions. It is a sub-class of `tornado.httpclient.HTTPError`, and it therefore also contains the HTTP error message, response and error code.

`NotModified`: The document has not been modified since the last update.

`BadRequest`: The syntax of the request was invalid or could not be processed.

`NotFound`: The requested resource was not found.

`MethodNotAllowed`: The request was made using an incorrect request method; for example, a GET was used where a POST was required.

`Conflict`: The request failed because of a database conflict.

`PreconditionFailed`: Could not create database - a database with that name already exists.

`InternalServerError`: The request was invalid and failed, or an error occurred within the CouchDB server that prevented it from processing the request.

## License

Copyright (c) 2010-2013 Nephics AB  
MIT License, see the LICENSE file.
