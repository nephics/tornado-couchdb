""""Blocking and non-blocking (asynchronous) clients for CouchDB using
Tornado's httpclient.

This module wraps the CouchDB HTTP REST API and defines a common interface
for making blocking and non-blocking operations on a CouchDB.
"""

import copy
import functools
import json

import tornado.ioloop
from tornado import httpclient, gen

from tornado.escape import json_decode, url_escape


__all__ = ["BlockingCouch", "AsyncCouch", "CouchException", "NotModified",
           "BadRequest", "NotFound", "MethodNotAllowed", "Conflict",
           "PreconditionFailed", "InternalServerError"]

__version__ = '0.3.0'


def json_encode(value):
    """JSON-encodes the given Python object."""
    return json.dumps(value, allow_nan=False).replace("</", "<\\/")


class AsyncCouch(object):
    """Basic wrapper class for asynchronous operations on a CouchDB

    Example usage::

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
    an exception is raised at the callpoint using an appropriate sub-class of
    CouchException. This, both when calling with a callback function or when
    yielding to a Future (using gen.coroutine).
    """

    def __init__(self, db_name='', couch_url='http://127.0.0.1:5984/',
                 io_loop=None, **request_args):
        """Creates an `AsyncCouch`.

        All parameters are optional. Though `db_name` is required for most
        methods to work.

        Database name `db_name` may be set on init and changed later by
        `use()`. The url to the CouchDB including port number, with or
        without authentication credentials.

        The `io_loop` is passed to the AsyncHTTPClient, used for connecting.

        Keyword arguments in `request_args` are applied when making requests
        to the database. By default the request argument `use_gzip` is True.
        Accessing a local CouchDB it may be relevant to set `use_gzip` to
        False.

        The request arguments may include `auth_username` and `auth_password`
        for basic authentication. See `httpclient.HTTPRequest` for other
        possible arguments.
        """
        self.request_args = request_args
        self._closed = False
        self.io_loop = io_loop
        self._client = httpclient.AsyncHTTPClient(self.io_loop)
        self.use(db_name, couch_url)

    def use(self, db_name='', couch_url='http://127.0.0.1:5984/'):
        """Set database name `db_name` and `couch_url`.

        The `couch_url` should include port number and authentication
        credentials as necessary.
        """
        self.db_name = db_name
        if couch_url.endswith('/'):
            self.couch_url = couch_url
        else:
            self.couch_url = couch_url + '/'

    def close(self):
        """Closes the CouchDB client, freeing any resources used."""
        if not self._closed:
            self._client.close()
            self._closed = True

    #
    # Database operations
    #

    @gen.coroutine
    def create_db(self, db_name=None):
        """Creates a new database."""
        r = yield self._http_put(db_name or self.db_name)
        raise gen.Return(r)

    @gen.coroutine
    def delete_db(self, db_name=None):
        """Deletes the database."""
        r = yield self._http_delete(db_name or self.db_name)
        raise gen.Return(r)

    @gen.coroutine
    def list_dbs(self):
        """List names of databases."""
        r = yield self._http_get('_all_dbs')
        raise gen.Return(r)

    @gen.coroutine
    def info_db(self, db_name=None):
        """Get info about the database."""
        r = yield self._http_get(db_name or self.db_name)
        raise gen.Return(r)

    @gen.coroutine
    def pull_db(self, source, db_name=None, create_target=False):
        """Replicate changes from a source database to current (target)
        database.
        """
        body = json_encode({
            'source': source,
            'target': (db_name or self.db_name),
            'create_target': create_target
        })
        r = yield self._http_post('_replicate', body, request_timeout=120.0)
        raise gen.Return(r)

    @gen.coroutine
    def uuids(self, count=1):
        """Get one or more uuids."""
        r = yield self._http_get('_uuids?count={0}'.format(count))
        raise gen.Return(r['uuids'])

    #
    # Document operations
    #

    @gen.coroutine
    def get_doc(self, doc_id):
        """Get document with the given `doc_id`."""
        url = '{0}/{1}'.format(self.db_name, url_escape(doc_id))
        r = yield self._http_get(url)
        raise gen.Return(r)

    @gen.coroutine
    def has_doc(self, doc_id):
        """Check if document with the given `doc_id` exists.
        Returns True if document exists, returns False otherwise.
        """
        url = '{0}/{1}'.format(self.db_name, url_escape(doc_id))
        r = yield self._http_head(url)
        raise gen.Return(r['code'] == 200)

    @gen.coroutine
    def get_docs(self, doc_ids):
        """Get multiple documents with the given list of `doc_ids`.

        Response is a list with the requested documents, in same order as the
        provided document id's.

        If one or more documents are not found in the database, a NotFound
        exception is raised.
        """
        url = '{0}/_all_docs?include_docs=true'.format(self.db_name)
        body = json_encode({'keys': doc_ids})
        r = yield self._http_post(url, body)
        raise gen.Return([row['doc'] for row in r['rows']])

    @gen.coroutine
    def save_doc(self, doc):
        """Save/create a document to/in a given database. Response is a dict
        with id and rev of the saved doc.
        """
        body = json_encode(doc)
        if '_id' in doc:
            # create new document, or update an existing document
            url = '{0}/{1}'.format(self.db_name, url_escape(doc['_id']))
            r = yield self._http_put(url, body)
        else:
            # create a new document
            url = self.db_name
            r = yield self._http_post(url, body)
        raise gen.Return(r)

    @gen.coroutine
    def save_docs(self, docs, all_or_nothing=False):
        """Save/create multiple documents.
        Response is a list of dicts with id and rev of the saved docs.
        """
        # use bulk docs API to update the docs
        url = '{0}/_bulk_docs'.format(self.db_name)
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        r = yield self._http_post(url, body)
        raise gen.Return(r)

    @gen.coroutine
    def delete_doc(self, doc):
        """Delete a document.
        The `doc` shall be a dict, at least having the keys `_id` and `_rev`.
        """
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('Missing id or revision information in doc')
        url = '{0}/{1}?rev={2}'.format(
            self.db_name, url_escape(doc['_id']), doc['_rev'])
        r = yield self._http_delete(url)
        raise gen.Return(r)

    @gen.coroutine
    def delete_docs(self, docs, all_or_nothing=False):
        """Delete multiple documents.
        The `docs` shall be an array of dicts, each at least having the keys
        `_id` and `_rev`.
        """
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            raise KeyError('Missing id or revision information in one or '
                           'more docs')
        # make list of docs to mark as deleted
        deleted = [{'_id': doc['_id'], '_rev': doc['_rev'],
                    '_deleted': True} for doc in docs]
        # use bulk docs API to update the docs
        url = '{0}/_bulk_docs'.format(self.db_name)
        body = json_encode({'all_or_nothing': all_or_nothing,
                            'docs': deleted})
        r = yield self._http_post(url, body)
        raise gen.Return(r)

    @gen.coroutine
    def get_attachment(self, doc, attachment_name, mimetype=None):
        """Get document attachment.
        The parameter `doc` should at least contain an `_id` key.
        If mimetype is not specified, `doc` shall contain an `_attachments`
        key with info about the named attachment."""
        if '_id' not in doc:
            raise ValueError('Missing key named _id in doc')
        if not mimetype:
            # get mimetype from the doc
            if '_attachments' not in doc:
                raise ValueError('No attachments in doc, cannot get content'
                                 ' type of attachment')
            elif attachment_name not in doc['_attachments']:
                raise ValueError('Document does not have an attachment by'
                                 ' the given name')
            else:
                mimetype = doc['_attachments'][attachment_name]['content_type']
        url = '{0}/{1}/{2}'.format(self.db_name, url_escape(doc['_id']),
                                   url_escape(attachment_name))
        headers = {'Accept': mimetype}
        r = yield self._http_get(url, headers=headers)
        raise gen.Return(r)

    @gen.coroutine
    def save_attachment(self, doc, attachment):
        """Save an attachment to the specified doc.
        The attachment shall be a dict with keys: `mimetype`, `name`, `data`.
        The doc shall be a dict, at least having the key `_id`, and if doc is
        existing in the database, it shall also contain the key `_rev`"""
        if any(key not in attachment for key in ['mimetype', 'name', 'data']):
            raise KeyError('Attachment dict is missing one or more '
                           'required keys')
        url = '{0}/{1}/{2}{3}'.format(
            self.db_name, url_escape(doc['_id']),
            url_escape(attachment['name']),
            '?rev={0}'.format(doc['_rev']) if '_rev' in doc else '')
        headers = {'Content-Type': attachment['mimetype']}
        body = attachment['data']
        r = yield self._http_put(url, body, headers=headers)
        raise gen.Return(r)

    @gen.coroutine
    def delete_attachment(self, doc, attachment_name):
        """Delete a named attachment to the specified doc.
        The doc shall be a dict, at least with the keys: _id and _rev"""
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('Missing id or revision information in doc')
        else:
            url = '{0}/{1}/{2}?rev={3}'.format(
                self.db_name, url_escape(doc['_id']),
                url_escape(attachment_name), doc['_rev'])
        r = yield self._http_delete(url)
        raise gen.Return(r)

    @gen.coroutine
    def view(self, design_doc_name, view_name, **kwargs):
        """Query a pre-defined view in the specified design doc.
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
          stale="ok"
          stale="update_after"

        Reverse the output:
          descending=True
          descending=False  (default value)

        Note that the descending option is applied before any key filtering, so
        you may need to swap the values of the startkey and endkey options to
        get the expected results.

        Skip the specified number of docs in the query results:
          skip=<number>  (default value is 0)

        The group option controls whether the reduce function reduces to a set
        of distinct keys or to a single result row:
          group=True
          group=False  (default value)

          group_level=<number>

        Use the reduce function of the view:
          reduce=True  (default value)
          reduce=False

        Note that default value of reduce is True, only if a reduce function is
        defined for the view.

        Automatically fetch and include the document which emitted each view
        entry:
          include_docs=True
          include_docs=False  (default value)

        Determine whether the endkey is included in the result:
          inclusive_end=True  (default value)
          inclusive_end=False
        """
        url = '{0}/_design/{1}/_view/{2}'.format(
            self.db_name, design_doc_name, view_name)
        r = yield self._view(url, **kwargs)
        raise gen.Return(r)

    @gen.coroutine
    def view_all_docs(self, **kwargs):
        """Query the _all_docs view.
        Accepts the same keyword parameters as `view()`.
        """
        url = '{0}/_all_docs'.format(self.db_name)
        r = yield self._view(url, **kwargs)
        raise gen.Return(r)

    @gen.coroutine
    def temp_view(self, view_doc, **kwargs):
        """Query a temporary view.
        The view_doc parameter is a dict with the view's map and reduce
        functions."""
        url = '{0}/_temp_view'.format(self.db_name)
        r = yield self._view(url, body=view_doc, **kwargs)
        raise gen.Return(r)

    @gen.coroutine
    def _view(self, url, **kwargs):
        body = kwargs.get('body', {})
        options = []
        if kwargs:
            for key, value in kwargs.items():
                if key == 'body':
                    continue
                if key == 'keys':
                    body.update({'keys': value})
                else:
                    value = url_escape(
                        value if key in ('startkey_docid', 'endkey_docid')
                        else json_encode(value))
                    options.append('='.join([key, value]))
        if options:
            url = '{0}?{1}'.format(url, '&'.join(options))
        if body:
            r = yield self._http_post(url, json_encode(body))
        else:
            r = yield self._http_get(url)
        raise gen.Return(r)

    #
    # Basic http methods and utility functions
    #

    def _parse_response(self, resp):
        # decode the JSON body and check for errors
        obj = json_decode(resp.body)

        def to_code(err):
            return {'not_found': 404, 'conflict': 409}.get(err, 400)

        if isinstance(obj, list):
            # check if there is an error in the list of dicts,
            # raise the first error seen
            for item in obj:
                if 'error' in item:
                    raise relax_exception(httpclient.HTTPError(
                        to_code(item['error']), item['reason'], resp))

        elif 'error' in obj:
            raise relax_exception(httpclient.HTTPError(
                resp.code, obj['reason'], resp))

        elif 'rows' in obj:
            # check if there is an error in the result rows,
            # raise the first error seen
            for row in obj['rows']:
                if 'error' in row:
                    raise relax_exception(httpclient.HTTPError(
                        to_code(row['error']), row['error'], resp))
        return obj

    def _parse_headers(self, resp):
        headers = {"code": resp.code}
        headers.update(resp.headers)
        return headers

    def _test_closed(self):
        if self._closed:
            raise CouchException('Database connection is closed.')

    @gen.coroutine
    def _http_get(self, uri, headers=None):
        self._test_closed()
        if headers is None:
            headers = {}
        req_args = copy.deepcopy(self.request_args)
        req_args.setdefault('headers', {}).update(headers)
        if 'Accept' not in req_args['headers']:
            req_args['headers']['Accept'] = 'application/json'
            decode = True
        else:
            # not a JSON response, don't try to decode
            decode = False
        req = httpclient.HTTPRequest(self.couch_url + uri, method='GET',
                                     **req_args)
        try:
            resp = yield self._client.fetch(req)
        except httpclient.HTTPError as e:
            if not e.response:
                raise relax_exception(e)
            resp = e.response
        raise gen.Return(self._parse_response(resp) if decode else resp.body)

    @gen.coroutine
    def _http_post(self, uri, body, **kwargs):
        self._test_closed()
        req_args = copy.deepcopy(self.request_args)
        req_args.update(kwargs)
        req_args.setdefault('headers', {}).update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                                     body=body, **req_args)
        try:
            resp = yield self._client.fetch(req)
        except httpclient.HTTPError as e:
            if not e.response:
                raise relax_exception(e)
            resp = e.response
        raise gen.Return(self._parse_response(resp))

    @gen.coroutine
    def _http_put(self, uri, body='', headers=None):
        self._test_closed()
        if headers is None:
            headers = {}
        req_args = copy.deepcopy(self.request_args)
        req_args.setdefault('headers', {}).update(headers)
        if body and 'Content-Type' not in req_args['headers']:
            req_args['headers']['Content-Type'] = 'application/json'
        if 'Accept' not in req_args['headers']:
            req_args['headers']['Accept'] = 'application/json'
        req = httpclient.HTTPRequest(self.couch_url + uri, method='PUT',
                                     body=body, **req_args)
        try:
            resp = yield self._client.fetch(req)
        except httpclient.HTTPError as e:
            if not e.response:
                raise relax_exception(e)
            resp = e.response
        raise gen.Return(self._parse_response(resp))

    @gen.coroutine
    def _http_delete(self, uri):
        self._test_closed()
        req_args = copy.deepcopy(self.request_args)
        req_args.setdefault('headers', {}).update({
            'Accept': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                                     **req_args)
        try:
            resp = yield self._client.fetch(req)
        except httpclient.HTTPError as e:
            if not e.response:
                raise relax_exception(e)
            resp = e.response
        raise gen.Return(self._parse_response(resp))

    @gen.coroutine
    def _http_head(self, uri):
        if self._closed:
            raise CouchException('Database connection is closed.')
        req_args = copy.deepcopy(self.request_args)
        req = httpclient.HTTPRequest(self.couch_url + uri, method='HEAD',
            **req_args)
        try:
            resp = yield self._client.fetch(req)
        except httpclient.HTTPError as e:
            if not e.response:
                raise relax_exception(e)
            resp = e.response
        raise gen.Return(self._parse_headers(resp))


class BlockingCouch(AsyncCouch):
    """Basic wrapper class for blocking operations on a CouchDB.

    Example usage::

        import couch

        db = couch.BlockingCouch('mytestdb')
        db.create_db()
        r = db.save_doc({'msg': 'My first document'})
        doc = db.get_doc(r['id'])
        db.delete_doc(doc)

    For any methods of this class: If an error is returned from the database,
    an appropriate CouchException is raised.

    BlockingCouch is a wrapper for AsyncCouch, database calls are run in a
    seperate IOLoop.
    """

    def __init__(self, db_name='', couch_url='http://127.0.0.1:5984/',
                 **request_args):
        """Creates a `BlockingCouch`.

        All parameters are optional. Though `db_name` is required for most
        methods to work.

        Database name `db_name` may be set on init and changed later by
        `use()`. The url to the CouchDB including port number, with or
        without authentication credentials.

        Keyword arguments in `request_args` are applied when making requests
        to the database. By default the request argument `use_gzip` is True.
        Accessing a local CouchDB it may be relevant to set `use_gzip` to
        False.

        The request arguments may include `auth_username` and `auth_password`
        for basic authentication. See `httpclient.HTTPRequest` for other
        possible arguments.
        """

        io_loop = tornado.ioloop.IOLoop(make_current=False)
        AsyncCouch.__init__(self, db_name, couch_url, io_loop=io_loop,
                            **request_args)

    def close(self):
        """Closes the CouchDB client, freeing any resources used."""
        if not self._closed:
            AsyncCouch.close(self)
            self.io_loop.close()

    def __getattribute__(self, name):
        try:
            attr = object.__getattribute__(self, name)
        except AttributeError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                                 self.__class__.__name__, name))

        if name == 'close' or name.startswith('_') or not hasattr(
                attr, '__call__'):
            # a 'local' or internal attribute, or a non-callable
            return attr

        # it's an asynchronous callable
        # return a callable wrapper for the attribute that will
        # run in its own IOLoop
        def wrapper(clb, *args, **kwargs):
            fn = functools.partial(clb, *args, **kwargs)
            return self.io_loop.run_sync(fn)
        return functools.partial(wrapper, attr)


class CouchException(httpclient.HTTPError):
    """Base class for Couch specific exceptions"""

    def __init__(self, HTTPError, msg=None):
        httpclient.HTTPError.__init__(
            self, HTTPError.code, msg, HTTPError.response)


class NotModified(CouchException):
    """HTTP Error 304 (Not Modified)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError,
            'The document has not been modified since the last update.')


class BadRequest(CouchException):
    """HTTP Error 400 (Bad Request)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError,
            'The syntax of the request was invalid or could not be processed.')


class NotFound(CouchException):
    """HTTP Error 404 (Not Found)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError, 'The requested resource was not found.')


class MethodNotAllowed(CouchException):
    """HTTP Error 405 (Method Not Allowed)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError, 'The request was made using an incorrect request '
            'method; for example, a GET was used where a POST was required.')


class Conflict(CouchException):
    """HTTP Error 409 (Conflict)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError,
            'The request failed because of a database conflict.')


class PreconditionFailed(CouchException):
    """HTTP Error 412 (Precondition Failed)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError, 'Could not create database - '
            'a database with that name already exists.')


class InternalServerError(CouchException):
    """HTTP Error 500 (Internal Server Error)"""

    def __init__(self, HTTPError):
        CouchException.__init__(
            self, HTTPError, 'The request was invalid and failed, '
            'or an error occurred within the CouchDB server that '
            'prevented it from processing the request.')


def relax_exception(e):
    """Convert HTTPError exception to a Couch specific exception, if possible,
    or else return the unmodified exception."""
    if isinstance(e, httpclient.HTTPError):
        if e.code == 304:
            ce = NotModified(e)
        elif e.code == 400:
            ce = BadRequest(e)
        elif e.code == 404:
            ce = NotFound(e)
        elif e.code == 405:
            ce = MethodNotAllowed(e)
        elif e.code == 409:
            ce = Conflict(e)
        elif e.code == 412:
            ce = PreconditionFailed(e)
        elif e.code == 500:
            ce = InternalServerError(e)
        else:
            # other HTTP Error
            ce = CouchException(e)
    else:
        # unknown exception
        ce = e

    return ce
