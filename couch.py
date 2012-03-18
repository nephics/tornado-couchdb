'''Blocking and non-blocking client interfaces to CouchDB using Tornado's
builtin `httpclient`.

This module wraps the CouchDB HTTP REST API and defines a common interface
for making blocking and non-blocking operations on a CouchDB.
'''

import copy

from tornado import httpclient
from tornado.escape import json_decode, json_encode, url_escape


class BlockingCouch(object):
    '''Basic wrapper class for blocking operations on a CouchDB.

    Example usage::

        import couch

        db = couch.BlockingCouch('mydatabase')
        db.create_db()
        r = db.save_doc({'msg': 'My first document'})
        doc = db.get_doc(r['id'])
        db.delete_doc(doc)

    For any methods of this class: If an error is returned from the database,
    an appropriate CouchException is raised.
    '''

    def __init__(self, db_name='', couch_url='http://127.0.0.1:5984',
            **request_args):
        '''Creates a `BlockingCouch`.

        All parameters are optional. Though `db_name` is required for most
        methods to work.

        :arg string db_name: Database name
        :arg string couch_url: The url to the CouchDB including port number,
            but without authentication credentials.
        :arg keyword request_args: Arguments applied when making requests to
            the database. This may include `auth_username` and `auth_password`
            for basic authentication. See `httpclient.HTTPRequest` for other
            possible arguments.
            By default `use_gzip` is set to False. Accessing a non-local
            CouchDB it may be relevant to set `use_gzip` to True.
        '''
        request_args.update({'use_gzip': False})
        self.request_args = request_args
        self.client = httpclient.HTTPClient()
        self.couch_url = couch_url
        self.db_name = db_name

    #
    # Database operations
    #

    def create_db(self):
        '''Creates database'''
        return self._http_put('/' + self.db_name)

    def delete_db(self):
        '''Deletes database'''
        return self._http_delete('/' + self.db_name)

    def list_dbs(self):
        '''List names of databases'''
        return self._http_get('/_all_dbs')

    def info_db(self):
        '''Get info about the database'''
        return self._http_get('/' + self.db_name)

    def pull_db(self, source, create_target=False):
        '''Replicate changes from a source database to current (target)
        database'''
        body = json_encode({'source': source, 'target': self.db_name,
                'create_target': create_target})
        return self._http_post('/_replicate', body, request_timeout=120.0)

    def uuids(self, count=1):
        '''Get one or more uuids'''
        return self._http_get('/_uuids?count={0}'.format(count))['uuids']

    #
    # Document operations
    #

    def get_doc(self, doc_id):
        '''Get document with the given `doc_id`.'''
        url = '/{0}/{1}'.format(self.db_name, url_escape(doc_id))
        return self._http_get(url)

    def get_docs(self, doc_ids):
        '''Get multiple documents with the given list of `doc_ids`.

        Returns a list containing the documents, in same order as the provided
        document id's.

        If one or more documents are not found in the database, an exception
        is raised.
        '''
        url = '/{0}/_all_docs?include_docs=true'.format(self.db_name)
        body = json_encode({'keys': doc_ids})
        resp = self._http_post(url, body)
        return [row['doc'] for row in resp['rows']]

    def save_doc(self, doc):
        '''Save/create a document in the database.
        Returns a dict with id and rev of the saved doc.'''
        body = json_encode(doc)
        if '_id' in doc and '_rev' in doc:
            # update an existing document
            url = '/{0}/{1}'.format(self.db_name, url_escape(doc['_id']))
            return self._http_put(url, body)
        else:
            # save a new document
            url = '/' + self.db_name
            return self._http_post(url, body)

    def save_docs(self, docs, all_or_nothing=False):
        '''Save/create multiple documents.
        Returns a list of dicts with id and rev of the saved docs.'''
        # use bulk docs API to update the docs
        url = '/{0}/_bulk_docs'.format(self.db_name)
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        return self._http_post(url, body)

    def delete_doc(self, doc):
        '''Delete a document'''
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('Missing id or revision information in doc')
        url = '/{0}/{1}?rev={2}'.format(self.db_name, url_escape(doc['_id']),
                doc['_rev'])
        return self._http_delete(url)

    def delete_docs(self, docs, all_or_nothing=False):
        '''Delete multiple documents'''
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            raise KeyError('Missing id or revision information in one or more '
                    'docs')
        # make list of docs to mark as deleted
        deleted = [{'_id': doc['_id'], '_rev': doc['_rev'], '_deleted': True}
                for doc in docs]
        # use bulk docs API to update the docs
        url = '/{0}/_bulk_docs'.format(self.db_name)
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': deleted})
        return self._http_post(url, body)

    def get_attachment(self, doc, attachment_name, mimetype=None):
        '''Get document attachment.
        The parameter `doc` should at least contain an `_id` key.
        If mimetype is not specified, `doc` shall contain an `_attachments`
        key with info about the named attachment.'''
        if '_id' not in doc:
            raise ValueError('Missing key named _id in doc')
        if not mimetype:
            # get mimetype from the doc
            if '_attachments' not in doc:
                raise ValueError('No attachments in doc, cannot get content '
                        'type of attachment')
            elif attachment_name not in doc['_attachments']:
                raise ValueError('Document does not have an attachment by the '
                        'given name')
            else:
                mimetype = doc['_attachments'][attachment_name]['content_type']
        url = '/{0}/{1}/{2}'.format(self.db_name, url_escape(doc['_id']),
                url_escape(attachment_name))
        headers = {'Accept': mimetype}
        return self._http_get(url, headers=headers)

    def save_attachment(self, doc, attachment):
        '''Save an attachment to the specified doc.
        The attachment shall be a dict with keys: `mimetype`, `name`, `data`.
        The doc shall be a dict, at least having the key `_id`, and if doc is
        existing in the database, it shall also contain the key `_rev`'''
        if any(key not in attachment for key in ('mimetype', 'name', 'data')):
            raise KeyError('Attachment dict is missing one or more required '
                    'keys')
        url = '/{0}/{1}/{2}{3}'.format(self.db_name, url_escape(doc['_id']),
                url_escape(attachment['name']),
                '?rev={0}'.format(doc['_rev']) if '_rev' in doc else '')
        headers = {'Content-Type': attachment['mimetype']}
        body = attachment['data']
        return self._http_put(url, body, headers=headers)

    def delete_attachment(self, doc, attachment_name):
        '''Delete a named attachment to the specified doc.
        The doc shall be a dict, at least with the keys: _id and _rev'''
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('Missing id or revision information in doc')
        url = '/{0}/{1}/{2}?rev={3}'.format(self.db_name,
                url_escape(doc['_id']), url_escape(attachment_name),
                doc['_rev'])
        return self._http_delete(url)

    def view(self, design_doc_name, view_name, **kwargs):
        '''Query a pre-defined view in the specified design doc.
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
        '''
        url = '/{0}/_design/{1}/_view/{2}'.format(self.db_name,
                design_doc_name, view_name)
        return self._view(url, **kwargs)

    def view_all_docs(self, **kwargs):
        '''Query the _all_docs view.
        Accepts the same keyword parameters as `view()`.
        '''
        url = '/{0}/_all_docs'.format(self.db_name)
        return self._view(url, **kwargs)

    def _view(self, url, **kwargs):
        body = None
        options = []
        if kwargs:
            for key, value in kwargs.items():
                if key == 'keys':
                    body = json_encode({'keys': value})
                else:
                    value = url_escape(json_encode(value))
                    options.append('='.join([key, value]))
        if options:
            url = '{0}?{1}'.format(url, '&'.join(options))
        if body:
            return self._http_post(url, body)
        else:
            return self._http_get(url)

    #
    # Basic http methods and utility functions
    #

    def _parse_response(self, resp):
        # decode the JSON body and check for errors
        obj = json_decode(resp.body)

        if isinstance(obj, list):
            # check if there is an error in the list of dicts,
            # raise the first error seen
            for item in obj:
                if 'error' in item:
                    raise relax_exception(httpclient.HTTPError(
                            resp.code if item['error'] != 'not_found' else 404,
                            item['reason'], resp))

        elif 'error' in obj:
            raise relax_exception(httpclient.HTTPError(resp.code,
                    obj['reason'], resp))

        elif 'rows' in obj:
            # check if there is an error in the result rows,
            # raise the first error seen
            for row in obj['rows']:
                if 'error' in row:
                    raise relax_exception(httpclient.HTTPError(
                            resp.code if row['error'] != 'not_found' else 404,
                            row['error'], resp))
        return obj

    def _fetch(self, request, decode=True):
        try:
            resp = self.client.fetch(request)
        except httpclient.HTTPError as e:
            raise relax_exception(e)
        return self._parse_response(resp) if decode else resp.body

    def _http_get(self, uri, headers=None):
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
        return self._fetch(req, decode)

    def _http_post(self, uri, body, **kwargs):
        req_args = copy.deepcopy(self.request_args)
        req_args.update(kwargs)
        req_args.setdefault('headers', {}).update({
                'Accept': 'application/json',
                'Content-Type': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                body=body, **req_args)
        return self._fetch(req)

    def _http_put(self, uri, body='', headers=None):
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
        return self._fetch(req)

    def _http_delete(self, uri):
        req_args = copy.deepcopy(self.request_args)
        req_args.setdefault('headers', {}).update({
                'Accept': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                **req_args)
        return self._fetch(req)


class AsyncCouch(object):
    '''Basic wrapper class for asynchronous operations on a CouchDB

    Example usage::

        import couch

        class TestCouch(object):

            def __init_(self):
                self.db = couch.AsyncCouch('mydatabase')
                self.db.create_db(self.dbcreated)

            def dbcreated(self, r):
                self.db.save_doc({'msg': 'My first document'}, self.docsaved)

            def docsaved(self, r):
                self.db.get_doc(r['id'], self.gotdoc)

            def gotdoc(self, doc):
                self.db.delete_doc(doc)

    For any methods of this class: If an error is returned from the database,
    the argument to the callback will contain the appropriate CouchException.
    '''

    def __init__(self, db_name='', couch_url='http://127.0.0.1:5984',
            **request_args):
        '''Creates an `AsyncCouch`.

        All parameters are optional. Though `db_name` is required for most
        methods to work.

        :arg string db_name: Database name
        :arg string couch_url: The url to the CouchDB including port number,
            but without authentication credentials.
        :arg keyword request_args: Arguments applied when making requests to
            the database. This may include `auth_username` and `auth_password`
            for basic authentication. See `httpclient.HTTPRequest` for other
            possible arguments.
            By default `use_gzip` is set to False. Accessing a non-local
            CouchDB it may be relevant to set `use_gzip` to True.
        '''
        request_args.update({'use_gzip': False})
        self.request_args = request_args
        self.client = httpclient.AsyncHTTPClient()
        self.couch_url = couch_url
        self.db_name = db_name

    #
    # Database operations
    #

    def create_db(self, callback=None):
        '''Creates a new database'''
        self._http_put('/' + self.db_name, callback=callback)

    def delete_db(self, callback=None):
        '''Deletes the database'''
        self._http_delete('/' + self.db_name, callback=callback)

    def list_dbs(self, callback):
        '''List names of databases'''
        self._http_get('/_all_dbs', callback=callback)

    def info_db(self, callback):
        '''Get info about the database'''
        self._http_get('/' + self.db_name, callback=callback)

    def pull_db(self, source, callback=None, create_target=False):
        '''Replicate changes from a source database to current (target)
        database'''
        body = json_encode({'source': source, 'target': self.db_name,
                'create_target': create_target})
        self._http_post('/_replicate', body, callback=callback,
                request_timeout=120.0)

    def uuids(self, callback, count=1):
        '''Get one or more uuids'''
        cb = lambda r: callback(r if isinstance(r, Exception) else r['uuids'])
        self._http_get('/_uuids?count={0}'.format(count), callback=cb)

    #
    # Document operations
    #

    def get_doc(self, doc_id, callback):
        '''Get document with the given `doc_id`.'''
        url = '/{0}/{1}'.format(self.db_name, url_escape(doc_id))
        self._http_get(url, callback=callback)

    def get_docs(self, doc_ids, callback):
        '''Get multiple documents with the given list of `doc_ids`.

        Calls back with a list containing the documents, in same order as the
        provided document id's.

        If one or more documents are not found in the database, the call back
        will get an exception.
        '''
        url = '/{0}/_all_docs?include_docs=true'.format(self.db_name)
        body = json_encode({'keys': doc_ids})

        def get_docs_cb(resp):
            if isinstance(resp, Exception):
                callback(resp)
            else:
                callback([row['doc'] for row in resp['rows']])
        self._http_post(url, body, callback=get_docs_cb)

    def save_doc(self, doc, callback=None):
        '''Save/create a document to/in a given database. Calls back with
        a dict with id and rev of the saved doc.'''
        body = json_encode(doc)
        if '_id' in doc and '_rev' in doc:
            # update an existing document
            url = '/{0}/{1}'.format(self.db_name, url_escape(doc['_id']))
            self._http_put(url, body, callback=callback)
        else:
            # save a new document
            url = '/' + self.db_name
            self._http_post(url, body, callback=callback)

    def save_docs(self, docs, callback=None, all_or_nothing=False):
        '''Save/create multiple documents.
        Calls back with a list of dicts with id and rev of the saved docs.'''
        # use bulk docs API to update the docs
        url = '/{0}/_bulk_docs'.format(self.db_name)
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        self._http_post(url, body, callback=callback)

    def delete_doc(self, doc, callback=None):
        '''Delete a document'''
        if '_rev' not in doc or '_id' not in doc:
            callback(KeyError('Missing id or revision information in doc'))
        else:
            url = '/{0}/{1}?rev={2}'.format(self.db_name,
                    url_escape(doc['_id']), doc['_rev'])
            self._http_delete(url, callback=callback)

    def delete_docs(self, docs, callback=None, all_or_nothing=False):
        '''Delete multiple documents'''
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            callback(KeyError('Missing id or revision information in one or '
                    'more docs'))
        else:
            # make list of docs to mark as deleted
            deleted = [{'_id': doc['_id'], '_rev': doc['_rev'],
                    '_deleted': True} for doc in docs]
            # use bulk docs API to update the docs
            url = '/{0}/_bulk_docs'.format(self.db_name)
            body = json_encode({'all_or_nothing': all_or_nothing,
                    'docs': deleted})
            self._http_post(url, body, callback=callback)

    def get_attachment(self, doc, attachment_name, mimetype=None,
            callback=None):
        '''Get document attachment.
        The parameter `doc` should at least contain an `_id` key.
        If mimetype is not specified, `doc` shall contain an `_attachments`
        key with info about the named attachment.'''
        if '_id' not in doc:
            callback(ValueError('Missing key named _id in doc'))
        if not mimetype:
            # get mimetype from the doc
            if '_attachments' not in doc:
                callback(ValueError('No attachments in doc, cannot get content'
                        ' type of attachment'))
            elif attachment_name not in doc['_attachments']:
                callback(ValueError('Document does not have an attachment by'
                        ' the given name'))
            else:
                mimetype = doc['_attachments'][attachment_name]['content_type']
        url = '/{0}/{1}/{2}'.format(self.db_name, url_escape(doc['_id']),
                url_escape(attachment_name))
        headers = {'Accept': mimetype}
        self._http_get(url, headers=headers, callback=callback)

    def save_attachment(self, doc, attachment, callback=None):
        '''Save an attachment to the specified doc.
        The attachment shall be a dict with keys: `mimetype`, `name`, `data`.
        The doc shall be a dict, at least having the key `_id`, and if doc is
        existing in the database, it shall also contain the key `_rev`'''
        if any(key not in attachment for key in ['mimetype', 'name', 'data']):
            callback(KeyError('Attachment dict is missing one or more '
                    'required keys'))
        else:
            url = '/{0}/{1}/{2}{3}'.format(self.db_name,
                    url_escape(doc['_id']), url_escape(attachment['name']),
                    '?rev={0}'.format(doc['_rev']) if '_rev' in doc else '')
            headers = {'Content-Type': attachment['mimetype']}
            body = attachment['data']
            self._http_put(url, body, headers=headers, callback=callback)

    def delete_attachment(self, doc, attachment_name, callback=None):
        '''Delete a named attachment to the specified doc.
        The doc shall be a dict, at least with the keys: _id and _rev'''
        if '_rev' not in doc or '_id' not in doc:
            callback(KeyError('Missing id or revision information in doc'))
        else:
            url = '/{0}/{1}/{2}?rev={3}'.format(self.db_name,
                    url_escape(doc['_id']), url_escape(attachment_name),
                    doc['_rev'])
        self._http_delete(url, callback=callback)

    def view(self, design_doc_name, view_name, callback, **kwargs):
        '''Query a pre-defined view in the specified design doc.
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
        '''
        url = '/{0}/_design/{1}/_view/{2}'.format(self.db_name,
                design_doc_name, view_name)
        self._view(url, callback=callback, **kwargs)

    def view_all_docs(self, callback, **kwargs):
        '''Query the _all_docs view.
        Accepts the same keyword parameters as `view()`.
        '''
        url = '/{0}/_all_docs'.format(self.db_name)
        self._view(url, callback=callback, **kwargs)

    def _view(self, url, callback=None, **kwargs):
        body = None
        options = []
        if kwargs:
            for key, value in kwargs.items():
                if key == 'keys':
                    body = json_encode({'keys': value})
                else:
                    value = url_escape(json_encode(value))
                    options.append('='.join([key, value]))
        if options:
            url = '{0}?{1}'.format(url, '&'.join(options))
        if body:
            self._http_post(url, body, callback=callback)
        else:
            self._http_get(url, callback=callback)

    #
    # Basic http methods and utility functions
    #

    def _parse_response(self, resp, callback):
        # decode the JSON body and check for errors
        obj = json_decode(resp.body)

        if isinstance(obj, list):
            # check if there is an error in the list of dicts,
            # raise the first error seen
            for item in obj:
                if 'error' in item:
                    callback(relax_exception(httpclient.HTTPError(
                            resp.code if item['error'] != 'not_found' else 404,
                            item['reason'], resp)))
                    return

        elif 'error' in obj:
            callback(relax_exception(httpclient.HTTPError(resp.code,
                    obj['reason'], resp)))
            return

        elif 'rows' in obj:
            # check if there is an error in the result rows,
            # raise the first error seen
            for row in obj['rows']:
                if 'error' in row:
                    callback(relax_exception(httpclient.HTTPError(
                            resp.code if row['error'] != 'not_found' else 404,
                            row['error'], resp)))
                    return
        callback(obj)

    def _http_callback(self, resp, callback, decode=True):
        if not callback:
            return
        if resp.error and not resp.body:
            # error, with no response body, call back with exception
            callback(relax_exception(resp.error))
        elif decode:
            # decode JSON response body and pass it to the callback function
            self._parse_response(resp, callback)
        else:
            # pass the response body directly to the user callback function
            callback(resp.body)

    def _http_get(self, uri, headers=None, callback=None):
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
        cb = lambda resp: self._http_callback(resp, callback, decode=decode)
        self.client.fetch(req, cb)

    def _http_post(self, uri, body, callback=None, **kwargs):
        req_args = copy.deepcopy(self.request_args)
        req_args.update(kwargs)
        req_args.setdefault('headers', {}).update({
                'Accept': 'application/json',
                'Content-Type': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                body=body, **req_args)
        cb = lambda resp: self._http_callback(resp, callback)
        self.client.fetch(req, cb)

    def _http_put(self, uri, body='', headers=None, callback=None):
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
        cb = lambda resp: self._http_callback(resp, callback)
        self.client.fetch(req, cb)

    def _http_delete(self, uri, callback=None):
        req_args = copy.deepcopy(self.request_args)
        req_args.setdefault('headers', {}).update({
                'Accept': 'application/json'})
        req = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                **req_args)
        cb = lambda resp: self._http_callback(resp, callback)
        self.client.fetch(req, cb)


class CouchException(httpclient.HTTPError):
    '''Base class for Couch specific exceptions'''
    def __init__(self, HTTPError, msg=None):
        body = json_decode(HTTPError.response.body)
        message = msg or body.get('reason')
        httpclient.HTTPError.__init__(self, HTTPError.code, message, HTTPError.response)


class NotModified(CouchException):
    '''HTTP Error 304 (Not Modified)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError,
                'The document has not been modified since the last update.')


class BadRequest(CouchException):
    '''HTTP Error 400 (Bad Request)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The syntax of the request '
                'was invalid or could not be processed.')


class NotFound(CouchException):
    '''HTTP Error 404 (Not Found)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError,
                'The requested resource was not found.')


class MethodNotAllowed(CouchException):
    '''HTTP Error 405 (Method Not Allowed)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request was made using '
                'an incorrect request method; for example, a GET was used '
                'where a POST was required.')


class Conflict(CouchException):
    '''HTTP Error 409 (Conflict)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request failed because '
                'of a database conflict.')


class PreconditionFailed(CouchException):
    '''HTTP Error 412 (Precondition Failed)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'Could not create database - '
                'a database with that name already exists.')


class InternalServerError(CouchException):
    '''HTTP Error 500 (Internal Server Error)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request was invalid and '
                'failed, or an error occurred within the CouchDB server that '
                'prevented it from processing the request.')


def relax_exception(e, callback=None):
    '''Convert HTTPError exception to a Couch specific exception, if possible,
    or else return the unmodified exception.'''
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

    if callback:
        callback(ce)
    else:
        return ce
