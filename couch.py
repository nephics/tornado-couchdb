from tornado import httpclient
from tornado.escape import json_decode, json_encode, url_escape



class BlockingCouch(object):
    '''Basic wrapper class for blocking operations on a CouchDB'''

    def __init__(self, db_name, host='localhost', port=5984):
        self.couch_url = 'http://{0}:{1}'.format(host, port)
        self.client = httpclient.HTTPClient()
        self.db_name = db_name


   # Database operations

    def create_db(self, raise_error=True):
        '''Creates database'''
        return self._http_put(''.join(['/', self.db_name, '/']), raise_error=raise_error)

    def delete_db(self, raise_error=True):
        '''Deletes database'''
        return self._http_delete(''.join(['/', self.db_name, '/']), raise_error=raise_error)

    def list_dbs(self, raise_error=True):
        '''List names of databases'''
        return self._http_get('/_all_dbs', raise_error=raise_error)

    def info_db(self, raise_error=True):
        '''Get info about the database'''
        return self._http_get(''.join(['/', self.db_name, '/']), raise_error=raise_error)

    def pull_db(self, source, create_target=False, raise_error=True):
        '''Replicate changes from a source database to current (target) database'''
        body = json_encode({'source': source, 'target': self.db_name, 'create_target': create_target})
        return self._http_post('/_replicate', body, raise_error=raise_error, connect_timeout=120.0, request_timeout=120.0)

    def uuids(self, count=1):
        '''Get one or more uuids'''
        if count > 1:
            url = ''.join(['/_uuids?count=', str(count)])
        else:
            url = '/_uuids'
        return self._http_get(url)['uuids']


    # Document operations
    
    def list_docs(self, raise_error=True):
        '''Get dict with id and rev of all documents in the database.'''
        resp = self._http_get(''.join(['/', self.db_name, '/_all_docs']), raise_error=raise_error)
        return dict((row['id'], row['value']['rev']) for row in resp['rows'])

    def get_doc(self, doc_id, raise_error=True):
        '''Get document with the given id.'''
        url = ''.join(['/', self.db_name, '/', url_escape(doc_id)])
        return self._http_get(url, raise_error=raise_error)

    def get_docs(self, doc_ids, raise_error=True):
        '''Get multiple documents with the given id's'''
        url = ''.join(['/', self.db_name, '/_all_docs?include_docs=true'])
        body = json_encode({'keys': doc_ids})
        resp = self._http_post(url, body, raise_error=raise_error)
        return [row['doc'] if 'doc' in row else row for row in resp['rows']]

    def save_doc(self, doc, raise_error=True):
        '''Save/create a document in the database. Returns a dict with id
           and rev of the saved doc.'''
        body = json_encode(doc)
        if '_rev' in doc:
            # update an existing document
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id'])])
            return self._http_put(url, body, doc=doc, raise_error=raise_error)
        else:
            # save a new document
            url = ''.join(['/', self.db_name])
            return self._http_post(url, body, doc=doc, raise_error=raise_error)

    def save_docs(self, docs, all_or_nothing=False, raise_error=True):
        '''Save/create multiple documents. Returns a list of dicts with id
           and rev of the saved docs.'''
        # use bulk docs API to update the docs
        url = ''.join(['/', self.db_name, '/_bulk_docs'])
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        return self._http_post(url, body, raise_error=raise_error)
        
    def delete_doc(self, doc, raise_error=True):
        '''Delete a document'''
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('No id or revision information in doc')
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '?rev=', doc['_rev']])
        return self._http_delete(url, raise_error=raise_error)

    def delete_docs(self, docs, all_or_nothing=False, raise_error=True):
        '''Delete multiple documents'''
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            raise KeyError('No id or revision information in one or more docs')
        # mark docs as deleted
        deleted = {'_deleted': True}
        [doc.update(deleted) for doc in docs]
        # use bulk docs API to update the docs
        url = ''.join(['/', self.db_name, '/_bulk_docs'])
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        return self._http_post(url, body, raise_error=raise_error)

    def get_attachment(self, doc, attachment_name, mimetype=None, raise_error=True):
        '''Open a document attachment. The doc should at least contain an _id key.
           If mimetype is not specified, the doc shall contain _attachments key with
           info about the named attachment.'''
        if '_id' not in doc:
            raise ValueError('Missing key named _id in doc')
        if not mimetype:
            # get mimetype from the doc
            if '_attachments' not in doc:
                raise ValueError('No attachments in doc, cannot get content type of attachment')
            elif attachment_name not in doc['_attachments']:
                raise ValueError('Document does not have an attachment by the given name')
            else:
                mimetype = doc['_attachments'][attachment_name]['content_type']
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       url_escape(attachment_name)])
        headers = {'Accept': mimetype}
        return self._http_get(url, headers=headers, raise_error=raise_error)

    def save_attachment(self, doc, attachment, raise_error=True):
        '''Save an attachment to the specified doc. The attachment shall be
        a dict with keys: mimetype, name, data. The doc shall be a dict, at
        least having the key _id, and if doc is existing in the database,
        it shall also contain the key _rev'''
        if any(key not in attachment for key in ['mimetype', 'name', 'data']):
            raise KeyError('Attachment dict is missing one or more required keys')
        if '_rev' in doc:
            q = ''.join(['?rev=', doc['_rev']])
        else:
            q = ''
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       url_escape(attachment['name']), q])
        headers = {'Content-Type': attachment['mimetype']}
        body = attachment['data']
        return self._http_put(url, body, headers=headers, raise_error=raise_error)

    def delete_attachment(self, doc, attachment_name, raise_error=True):
        '''Delete an attachment to the specified doc. The attatchment shall be
           a dict with keys: mimetype, name, data. The doc shall be a dict, at
           least with the keys: _id and _rev'''
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('No id or revision information in doc')
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       attachment_name, '?rev=', doc['_rev']])
        return self._http_delete(url, raise_error=raise_error)

    def view(self, design_doc_name, view_name, raise_error=True, **kwargs):
        '''Query a pre-defined view in the specified design doc.
        The following query parameters can be specified as keyword arguments.

        Limit query results to those with the specified key or list of keys
          key=<key-value>
          keys=<list of keys>
          
        Limit query results to those following the specified startkey
          startkey=<key-value>
          
        First document id to include in the output
          startkey_docid=<document id>
          
        Limit query results to those previous to the specified endkey
          endkey=<key-value>
          
        Last document id to include in the output
          endkey_docid=<document id>
          
        Limit the number of documents in the output
          limit=<number of docs>
          
        If stale=ok is set CouchDB will not refresh the view even if it is stalled.
          stale=ok
          
        Reverse the output (default is false). Note that the descending option is
        applied before any key filtering, so you may need to swap the values of the
        startkey and endkey options to get the expected results.
          descending=true
          descending=false
          
        Skip the specified number of docs in the query results:
          skip=<number>
          
        The group option controls whether the reduce function reduces to a set of
        distinct keys or to a single result row:
          group=true
          group=false
        
          group_level=<number>
          
        Use the reduce function of the view. It defaults to true, if a reduce
        function is defined and to false otherwise.
          reduce=true
          reduce=false
        
        Automatically fetch and include the document which emitted each view
        entry (default is false).
          include_docs=true
          include_docs=false
        
        Controls whether the endkey is included in the result. It defaults to true.
          inclusive_end=true
          inclusive_end=false
        '''
        body = None
        options = []
        if kwargs:
            for key, value in kwargs.iteritems():
                if key == 'keys':
                    body = json_encode({'keys': value})
                else:
                    value = url_escape(json_encode(value))
                    options.append('='.join([key, value]))
        if options:
            q = '?' + '&'.join(options)
        else:
            q = ''
        url = ''.join(['/', self.db_name, '/_design/', design_doc_name, '/_view/', view_name, q])
        if body:
            return self._http_post(url, body, raise_error=raise_error)
        else:
            return self._http_get(url, raise_error=raise_error)


    # Basic http methods and utility functions

    def _parse_response(self, resp, raise_error=True):
        # the JSON body and check for errors
        obj = json_decode(resp.body)
        if raise_error:
            if 'error' in obj:
                raise relax_exception(httpclient.HTTPError(resp.code, obj['reason'], resp))
            elif isinstance(obj, list):
                # check if there is an error in the list of dicts, raise the first error seen
                for item in obj:
                    if 'error' in item:
                        raise relax_exception(httpclient.HTTPError(resp.code, item['reason'], resp))
            elif 'rows' in obj:
                # check if there is an error in the result rows, raise the first error seen
                for row in obj['rows']:
                    if 'error' in row:
                        raise relax_exception(httpclient.HTTPError(resp.code, row['error'], resp))
        return obj

    def _fetch(self, request, raise_error, decode=True):
        try:
            resp = self.client.fetch(request)
        except httpclient.HTTPError as e:
            if raise_error:
                raise relax_exception(e)
            else:
                return json_decode(e.response.body)

        if decode:
            return self._parse_response(resp, raise_error)
        else:
            return resp.body

    def _http_get(self, uri, headers=None, raise_error=True):
        if not isinstance(headers, dict):
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
            decode = True
        else:
            # not a JSON response, don't try to decode 
            decode = False
        r = httpclient.HTTPRequest(self.couch_url + uri, method='GET',
                                   headers=headers, use_gzip=False)
        return self._fetch(r, raise_error, decode)

    def _http_post(self, uri, body, doc=None, raise_error=True, **kwargs):
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        r = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                                   headers=headers, body=body,
                                   use_gzip=False, **kwargs)
        return self._fetch(r, raise_error)

    def _http_put(self, uri, body='', headers=None, doc=None, raise_error=True):
        if not isinstance(headers, dict):
            headers = {}
        if body and 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        r = httpclient.HTTPRequest(self.couch_url + uri, method='PUT',
                                   headers=headers, body=body, use_gzip=False)
        return self._fetch(r, raise_error)

    def _http_delete(self, uri, raise_error=True):
        r = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                                   headers={'Accept': 'application/json'},
                                   use_gzip=False)
        return self._fetch(r, raise_error)



class AsyncCouch(object):
    '''Basic wrapper class for asynchronous operations on a CouchDB'''

    def __init__(self, db_name, host='localhost', port=5984):
        self.couch_url = 'http://{0}:{1}'.format(host, port)
        self.client = httpclient.AsyncHTTPClient()
        self.db_name = db_name


    # Database operations

    def create_db(self, callback=None):
        '''Creates a new database'''
        self._http_put(''.join(['/', self.db_name, '/']), '', callback=callback)

    def delete_db(self, callback=None):
        '''Deletes the database'''
        self._http_delete(''.join(['/', self.db_name, '/']), callback=callback)

    def list_dbs(self, callback=None):
        '''List the databases on the server'''
        self._http_get('/_all_dbs', callback=callback)

    def info_db(self, callback=None):
        '''Get info about the database'''
        self._http_get(''.join(['/', self.db_name, '/']), callback=callback)

    def pull_db(self, source, callback=None, create_target=False):
        '''Replicate changes from a source database to current (target) db'''
        body = json_encode({'source': source, 'target': self.db_name, 'create_target': create_target})
        self._http_post('/_replicate', body, callback=callback, connect_timeout=120.0, request_timeout=120.0)

    def uuids(self, count=1, callback=None):
        def uuids_cb(resp):
            if callback:
                if isinstance(resp, Exception):
                    callback(resp)
                else:
                    callback(resp['uuids'])
        if count > 1:
            url = ''.join(['/_uuids?count=', str(count)])
        else:
            url = '/_uuids'
        self._http_get(url, callback=uuids_cb)


    # Document operations
    
    def list_docs(self, callback=None):
        '''Get dict with id and rev of all documents in the database'''
        def list_docs_cb(resp):
            if isinstance(resp, Exception):
                callback(resp)
            else:
                callback(dict((row['id'], row['value']['rev']) for row in resp['rows']))
        self._http_get(''.join(['/', self.db_name, '/_all_docs']), callback=callback)

    def get_doc(self, doc_id, callback=None):
        '''Open a document with the given id'''
        url = ''.join(['/', self.db_name, '/', url_escape(doc_id)])
        self._http_get(url, callback=callback)

    def get_docs(self, doc_ids, callback=None):
        '''Get multiple documents with the given id's'''
        url = ''.join(['/', self.db_name, '/_all_docs?include_docs=true'])
        body = json_encode({'keys': doc_ids})
        def get_docs_cb(resp):
            if isinstance(resp, Exception):
                callback(resp)
            else:
                callback([row['doc'] if 'doc' in row else row for row in resp['rows']])
        self._http_post(url, body, callback=get_docs_cb)

    def save_doc(self, doc, callback=None):
        '''Save/create a document to/in a given database. Calls back with
           a dict with id and rev of the saved doc.'''
        body = json_encode(doc)
        if '_id' in doc and '_rev' in doc:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id'])])
            self._http_put(url, body, doc=doc, callback=callback)
        else:
            url = ''.join(['/', self.db_name])
            self._http_post(url, body, doc=doc, callback=callback)

    def save_docs(self, docs, callback=None, all_or_nothing=False):
        '''Save/create multiple documents. Calls back with a list of dicts with id
           and rev of the saved docs.'''
        # use bulk docs API to update the docs
        url = ''.join(['/', self.db_name, '/_bulk_docs'])
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        self._http_post(url, body, callback=callback)

    def delete_doc(self, doc, callback=None):
        '''Delete a document'''
        if '_rev' not in doc or '_id' not in doc:
            callback(KeyError('No id or revision information in doc'))
        else:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '?rev=', doc['_rev']])
            self._http_delete(url, callback=callback)

    def delete_docs(self, docs, callback=None, all_or_nothing=False):
        '''Delete multiple documents'''
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            callback(KeyError('No id or revision information in one or more docs'))
        else:
            # mark docs as deleted
            map(lambda doc: doc.update({'_deleted': True}), docs)
            # use bulk docs API to update the docs
            url = ''.join(['/', self.db_name, '/_bulk_docs'])
            body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
            self._http_post(url, body, callback=callback)
        
    def get_attachment(self, doc, attachment_name, mimetype=None, callback=None):
        '''Open a document attachment. The doc should at least contain an _id key.
           If mimetype is not specified, the doc shall contain _attachments key with
           info about the named attachment.'''
        if '_id' not in doc:
            callback(ValueError('Missing key named _id in doc'))
        if not mimetype:
            # get mimetype from the doc
            if '_attachments' not in doc:
                callback(ValueError('No attachments in doc, cannot get content type of attachment'))
            elif attachment_name not in doc['_attachments']:
                callback(ValueError('Document does not have an attachment by the given name'))
            else:
                mimetype = doc['_attachments'][attachment_name]['content_type']
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                           url_escape(attachment_name)])
            headers = {'Accept': mimetype}
            self._http_get(url, headers=headers, callback=callback)

    def save_attachment(self, doc, attachment, callback=None):
        '''Save an attachment to the specified doc. The attachment shall be
        a dict with keys: mimetype, name, data. The doc shall be a dict, at
        least having the key _id, and if doc is existing in the database,
        it shall also contain the key _rev'''
        if any(key not in attachment for key in ['mimetype', 'name', 'data']):
            callback(KeyError('Attachment dict is missing one or more required keys'))
        else:
            if '_rev' in doc:
                q = ''.join(['?rev=', doc['_rev']])
            else:
                q = ''
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                           url_escape(attachment['name']), q])
            headers = {'Content-Type': attachment['mimetype']}
            self._http_put(url, body=attachment['data'], headers=headers, callback=callback)

    def delete_attachment(self, doc, attachment_name, callback=None):
        '''Delete an attachment to the specified doc. The attatchment shall be
           a dict with keys: mimetype, name, data. The doc shall be a dict, at
           least with the keys: _id and _rev'''
        if '_rev' not in doc or '_id' not in doc:
            callback(KeyError('No id or revision information in doc'))
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       attachment_name, '?rev=', doc['_rev']])
        self._http_delete(url, callback=callback)

    def view(self, design_doc_name, view_name, callback=None, **kwargs):
        '''Query a pre-defined view in the specified design doc.
        The following query parameters can be specified as keyword arguments.

        Limit query results to those with the specified key or list of keys
          key=<key-value>
          keys=<list of keys>
          
        Limit query results to those following the specified startkey
          startkey=<key-value>
          
        First document id to include in the output
          startkey_docid=<document id>
          
        Limit query results to those previous to the specified endkey
          endkey=<key-value>
          
        Last document id to include in the output
          endkey_docid=<document id>
          
        Limit the number of documents in the output
          limit=<number of docs>
          
        If stale=ok is set CouchDB will not refresh the view even if it is stalled.
          stale=ok
          
        Reverse the output (default is false). Note that the descending option is
        applied before any key filtering, so you may need to swap the values of the
        startkey and endkey options to get the expected results.
          descending=true
          descending=false
          
        Skip the specified number of docs in the query results:
          skip=<number>
          
        The group option controls whether the reduce function reduces to a set of
        distinct keys or to a single result row:
          group=true
          group=false
        
          group_level=<number>
          
        Use the reduce function of the view. It defaults to true, if a reduce
        function is defined and to false otherwise.
          reduce=true
          reduce=false
        
        Automatically fetch and include the document which emitted each view
        entry (default is false).
          include_docs=true
          include_docs=false
        
        Controls whether the endkey is included in the result. It defaults to true.
          inclusive_end=true
          inclusive_end=false
        '''
        body = None
        options = []
        if kwargs:
            for key, value in kwargs.iteritems():
                if key == 'keys':
                    body = json_encode({'keys': value})
                else:
                    value = url_escape(json_encode(value))
                    options.append('='.join([key, value]))
        if options:
            q = '?' + '&'.join(options)
        else:
            q = ''
        url = ''.join(['/', self.db_name, '/_design/', design_doc_name, '/_view/', view_name, q])
        if body:
            self._http_post(url, body, callback=callback)
        else:
            self._http_get(url, callback=callback)
        
    # Basic http methods

    def _http_callback(self, resp, callback, decode=True):
        if not callback:
            return
        if resp.error and not resp.body:
            # error, with no response body, call back with exception
            callback(relax_exception(resp.error))
        elif decode:
            # decode the JSON body and pass to the user callback function
            obj = json_decode(resp.body)
            if 'error' in obj:
                callback(relax_exception(httpclient.HTTPError(resp.code, obj['reason'], resp)))
            else:
                callback(obj)
        else:
            # pass the response body directly to the user callback function
            callback(resp.body)

    def _http_get(self, uri, headers=None, callback=None):
        if not isinstance(headers, dict):
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
            decode = True
        else:
            # user callback shall take perform decoding, as required
            decode = False
        r = httpclient.HTTPRequest(self.couch_url + uri, method='GET',
                                   headers=headers, use_gzip=False)
        
        cb = lambda resp: self._http_callback(resp, callback, decode=decode)
        self.client.fetch(r, cb)

    def _http_post(self, uri, body, doc=None, callback=None, **kwargs):
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        r = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                                   headers=headers, body=body,
                                   use_gzip=False, **kwargs)
        self.client.fetch(r, lambda resp: self._http_callback(resp, callback,
                                                              doc=doc))

    def _http_put(self, uri, body, headers=None, callback=None, doc=None):
        if not isinstance(headers, dict):
            headers = {}
        if 'Content-Type' not in headers and len(body) > 0:
            headers['Content-Type'] = 'application/json'
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        r = httpclient.HTTPRequest(self.couch_url + uri, method='PUT',
                                   headers=headers, body=body, use_gzip=False)
        self.client.fetch(r, lambda resp: self._http_callback(resp, callback,
                                                              doc=doc))

    def _http_delete(self, uri, callback=None):
        r = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                                   headers={'Accept': 'application/json'},
                                   use_gzip=False)
        self.client.fetch(r, lambda resp: self._http_callback(resp, callback))


class CouchException(httpclient.HTTPError):
    '''Base class for Couch specific exceptions'''
    def __init__(self, HTTPError, msg):
        httpclient.HTTPError.__init__(self, HTTPError.code, msg, HTTPError.response)

class NotModified(CouchException):
    '''HTTP Error 304 (Not Modified)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The document has not been modified since the last update.')

class BadRequest(CouchException):
    '''HTTP Error 400 (Bad Request)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The syntax of the request was invalid or could not be processed.')

class NotFound(CouchException):
    '''HTTP Error 404 (Not Found)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The requested resource was not found.')

class MethodNotAllowed(CouchException):
    '''HTTP Error 405 (Method Not Allowed)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request was made using an incorrect request method; '
                                'for example, a GET was used where a POST was required.')

class Conflict(CouchException):
    '''HTTP Error 409 (Conflict)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request failed because of a database conflict.')

class PreconditionFailed(CouchException):
    '''HTTP Error 412 (Precondition Failed)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'Could not create database - a database with that name already exists.')

class InternalServerError(CouchException):
    '''HTTP Error 500 (Internal Server Error)'''
    def __init__(self, HTTPError):
        CouchException.__init__(self, HTTPError, 'The request was invalid and failed, or an error '
                                'occurred within the CouchDB server that prevented it from processing the request.')

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
            # unknown HTTP Error
            ce = e
    else:
        # unknown exception
        ce = e
        
    if callback:
        callback(ce)
    else:
        return ce


