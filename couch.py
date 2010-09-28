from tornado import httpclient
from tornado.escape import json_decode, json_encode, url_escape

class BlockingCouch(object):
    '''Basic wrapper class for blocking operations on a CouchDB'''

    def __init__(self, db_name, host='localhost', port=5984):
        self.couch_url = 'http://{0}:{1}'.format(host, port)
        self.client = httpclient.HTTPClient()
        self.db_name = db_name

    # Database operations

    def create_db(self):
        '''Creates the database on the server'''
        return self._http_put(''.join(['/', self.db_name, '/']), '')

    def delete_db(self):
        '''Deletes the database on the server'''
        return self._http_delete(''.join(['/', self.db_name, '/']))

    def list_dbs(self):
        '''List the databases on the server'''
        return self._http_get('/_all_dbs')

    def info_db(self):
        '''Returns info about the database'''
        return self._http_get(''.join(['/', self.db_name, '/']))
        
    def uuids(self, count=1):
        if count > 1:
            q = ''.join(['?count=', str(count)])
        else:
            q = ''
        return self._http_get(''.join(['/_uuids', q]))['uuids']

    # Document operations
    
    def list_docs(self):
        '''List all documents in a given database'''
        return self._http_get(''.join(['/', self.db_name, '/_all_docs']))

    def get_doc(self, doc_id):
        '''Open a document with the given id'''
        url = ''.join(['/', self.db_name, '/', url_escape(doc_id)])
        return self._http_get(url)

    def get_docs(self, doc_ids):
        '''Get multiple documents with the given id's'''
        url = ''.join(['/', self.db_name, '/_all_docs?include_docs=true'])
        body = json_encode({'keys': doc_ids})
        return self._http_post(url, body)
        
    def save_doc(self, doc):
        '''Save/create a document to/in a given database. On success, a copy of
        the doc is returned with updated _id and _rev keys set.'''
        body = json_encode(doc)
        if '_rev' in doc:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id'])])
            return self._http_put(url, body, doc=doc)
        else:
            url = ''.join(['/', self.db_name])
            return self._http_post(url, body, doc=doc)

    def save_docs(self, docs, all_or_nothing=False):
        '''Save/create multiple documents'''
        # use bulk docs API to update the docs
        url = ''.join(['/', self.db_name, '/_bulk_docs'])
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        return self._http_post(url, body)

    def delete_doc(self, doc):
        '''Delete a document'''
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('No id or revision information in doc')
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '?rev=', doc['_rev']])
        return self._http_delete(url)

    def delete_docs(self, docs, all_or_nothing=False):
        '''Delete multiple documents'''
        if any('_rev' not in doc or '_id' not in doc for doc in docs):
            raise KeyError('No id or revision information in one or more docs')
        # mark docs as deleted
        map(lambda doc: doc.update({'_deleted': True}), docs)
        # use bulk docs API to update the docs
        url = ''.join(['/', self.db_name, '/_bulk_docs'])
        body = json_encode({'all_or_nothing': all_or_nothing, 'docs': docs})
        return self._http_post(url, body)
        
    def get_attachment(self, doc, attachment_name):
        '''Open a document attachment'''
        if '_id' not in doc:
            raise ValueError('Missing key named _id in doc')
        if '_attachments' not in doc or attachment_name not in doc['_attachments']:
            raise ValueError('Document does not have an attachment with the '
                             'specified name')
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       url_escape(attachment_name)])
        headers = {'Accept': doc['_attachments'][attachment_name]['content_type']}
        return self._http_get(url, headers=headers)

    def save_attachment(self, doc, attachment):
        '''Save an attachment to the specified doc. The attatchment shall be
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

        return self._http_put(url, body=attachment['data'], headers=headers)

    def delete_attachment(self, doc, attachment_name):
        if '_rev' not in doc or '_id' not in doc:
            raise KeyError('No id or revision information in doc')
        if '_attachments' not in doc or attachment_name not in doc['_attachments']:
            raise ValueError('Document does not have an attachment with the '
                             'specified name')
        url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                       attachment_name, '?rev=', doc['_rev']])
        return self._http_delete(url)

    def view(self, design_doc_name, view_name, **kwargs):
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
            return self._http_post(url, body)
        else:
            return self._http_get(url)
        
    # Basic http methods

    def _parse_response(self, resp, decode=True, doc=None):
        if decode:
            # decode the JSON body before returning result
            obj = json_decode(resp.body)
            if 'error' in obj:
                raise relax_exception(httpclient.HTTPError(resp.code, obj['reason'], resp))
            if doc:
                # modify doc _id and _rev keys according to the response
                new_doc = dict(doc)
                new_doc.update({'_id': obj['id'], '_rev': obj['rev']})
                return new_doc
            else:
                return obj
        else:
            # return the response body
            return resp.body

    def _http_get(self, uri, headers=None):
        if not isinstance(headers, dict):
            headers = {}
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
            decode = True
        else:
            # user callback shall take perform decoding, as
            decode = False
        r = httpclient.HTTPRequest(self.couch_url + uri, method='GET',
                                   headers=headers, use_gzip=False)
        try:
            resp = self.client.fetch(r)
        except httpclient.HTTPError, e:
            raise relax_exception(e)

        return self._parse_response(resp, decode=decode)

    def _http_post(self, uri, body, doc=None):
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        r = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                                   headers=headers,
                                   body=body, use_gzip=False)
        try:
            resp = self.client.fetch(r)
        except httpclient.HTTPError, e:
            raise relax_exception(e)

        return self._parse_response(resp, doc=doc)

    def _http_put(self, uri, body, headers=None, doc=None):
        if not isinstance(headers, dict):
            headers = {}
        if 'Content-Type' not in headers and len(body) > 0:
            headers['Content-Type'] = 'application/json'
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'
        r = httpclient.HTTPRequest(self.couch_url + uri, method='PUT',
                                   headers=headers, body=body, use_gzip=False)
        try:
            resp = self.client.fetch(r)
        except httpclient.HTTPError, e:
            raise relax_exception(e)
        
        return self._parse_response(resp, doc=doc)

    def _http_delete(self, uri):
        r = httpclient.HTTPRequest(self.couch_url + uri, method='DELETE',
                                   headers={'Accept': 'application/json'},
                                   use_gzip=False)
        try:
            resp = self.client.fetch(r)
        except httpclient.HTTPError, e:
            raise relax_exception(e)

        return self._parse_response(resp)
        
class AsyncCouch(object):
    '''Basic wrapper class for asynchronous operations on a CouchDB'''
    def __init__(self, db_name, host='localhost', port=5984):
        self.couch_url = 'http://{0}:{1}'.format(host, port)
        self.client = httpclient.AsyncHTTPClient()
        self.db_name = db_name

    # Database operations

    def create_db(self, callback=None):
        '''Creates a new database on the server'''
        self._http_put(''.join(['/', self.db_name, '/']), '', callback=callback)

    def delete_db(self, callback=None):
        '''Deletes the database on the server'''
        self._http_delete(''.join(['/', self.db_name, '/']), callback=callback)

    def list_dbs(self, callback=None):
        '''List the databases on the server'''
        self._http_get('/_all_dbs', callback=callback)

    def info_db(self, callback=None):
        '''Returns info about the database'''
        self._http_get(''.join(['/', self.db_name, '/']), callback=callback)

    def uuids(self, count=1, callback=None):
        def got_uuids(result):
            if not callback:
                return 
            if isinstance(result, Exception):
                callback(result)
            else:
                callback(result['uuids'])
        if count > 1:
            q = ''.join(['?count=', str(count)])
        else:
            q = ''
        url = ''.join(['/_uuids', q])
        self._http_get(url, callback=got_uuids)

    # Document operations
    
    def list_docs(self, callback=None):
        '''List all documents in a given database'''
        self._http_get(''.join(['/', self.db_name, '/_all_docs']),
                      callback=callback)

    def get_doc(self, doc_id, callback=None):
        '''Open a document with the given id'''
        self._http_get(''.join(['/', self.db_name, '/', url_escape(doc_id)]), callback=callback)

    def get_docs(self, doc_ids, callback=None):
        '''Get multiple documents with the given id's'''
        url = ''.join(['/', self.db_name, '/_all_docs?include_docs=true'])
        body = json_encode({'keys': doc_ids})
        self._http_post(url, body, callback=callback)

    def save_doc(self, doc, callback=None):
        '''Save/create a document to/in a given database. On success, the
        callback is passed a copy of the doc with updated _id and _rev keys set.'''
        body = json_encode(doc)
        if '_id' in doc and '_rev' in doc:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id'])])
            self._http_put(url, body, doc=doc, callback=callback)
        else:
            url = ''.join(['/', self.db_name])
            self._http_post(url, body, doc=doc, callback=callback)

    def save_docs(self, docs, callback=None, all_or_nothing=False):
        '''Save/create multiple documents'''
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
        
    def get_attachment(self, doc, attachment_name, callback=None):
        '''Open a document attachment'''
        if '_id' not in doc:
            callback(ValueError('Missing key named _id in doc'))
        elif '_attachments' not in doc or attachment_name not in doc['_attachments']:
            callback(ValueError('Document does not have an attachment with the '
                                'specified name'))
        else:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                           url_escape(attachment_name)])
            headers = {'Accept': doc['_attachments'][attachment_name]['content_type']}
            self._http_get(url, headers=headers, callback=callback)

    def save_attachment(self, doc, attachment, callback=None):
        '''Save an attachment to the specified doc. The attatchment shall be
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
        if '_rev' not in doc or '_id' not in doc:
            callback(KeyError('No id or revision information in doc'))
        elif '_attachments' not in doc or attachment_name not in doc['_attachments']:
            callback(ValueError('Document does not have an attachment with the '
                                'specified name'))
        else:
            url = ''.join(['/', self.db_name, '/', url_escape(doc['_id']), '/',
                           attachment_name, '?rev=', doc['_rev']])
            self._http_delete(url, callback=callback)

    def view(self, design_doc_name, view_name, callback=None, **kwargs):
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

    def _http_callback(self, resp, callback, decode=True, doc=None):
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
                
            elif doc:
                # modify doc _id and _rev keys according to the response
                new_doc = dict(doc)
                new_doc.update({'_id': obj['id'], '_rev': obj['rev']})
                callback(new_doc)
                
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

    def _http_post(self, uri, body, doc=None, callback=None):
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
        r = httpclient.HTTPRequest(self.couch_url + uri, method='POST',
                                   headers=headers,
                                   body=body, use_gzip=False)
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


