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
    
    db = couch.BlockingCouch('mydatabase')
    db.create_db()
    r = db.save_doc({'msg': 'My first document'})
    doc = db.get_doc(r['id'])
    db.delete_doc(doc)


For any methods of this class: If an error is returned from the database,
an exception is raised using an appropriate sub-class of CouchException.

## AsyncCouch

The AsyncCouch class is a basic wrapper for making non-blocking operations on a CouchDB. Using this class implies that the Tornado eventloop is *not* blocked on  a database call, and reply from the database is delivered to a callback function.

Use this class when blocking the eventloop *is* a problem, or when there is a *high latency connection* to the database, and the operations are "large", i.e., takes long time (in the range of seconds) to complete.

Example usage:

    import couch

    class TestCouch(object):

        def __init_(self, dbname):
            self.db = couch.AsyncCouch(dbname)
            self.db.create_db(self.dbcreated)

        def dbcreated(self, r):
            self.db.save_doc({'msg': 'My first document'}, self.docsaved)

        def docsaved(self, r):
            self.db.get_doc(r['id'], self.gotdoc)

        def gotdoc(self, doc):
            self.db.delete_doc(doc)

    TestCouch('mydatabase')

For any methods of this class: If an error is returned from the database,
the argument to the callback will contain the appropriate sub-class of CouchException.

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
