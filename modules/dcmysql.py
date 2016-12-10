#!/usr/bin/env python
#
# Data Collection WS - prototype
#
# (c) 2016 Javier Quinteros, GEOFON team
# <javier@gfz-potsdam.de>
#
# ----------------------------------------------------------------------

"""Data Collection WS - prototype

   :Platform:
       Linux
   :Copyright:
       GEOFON, GFZ Potsdam <geofon@gfz-potsdam.de>
   :License:
       GNU General Public License v3

.. moduleauthor:: Javier Quinteros <javier@gfz-potsdam.de>, GEOFON, GFZ Potsdam
"""

import os
import logging
import json
import datetime
import configparser
import cgi
import MySQLdb
from collections import namedtuple
from wsgicomm import WINotFoundError
from wsgicomm import WIClientError
from wsgicomm import WICreated

# For the time being these are the capabilities for the immutable datasets
# coming from the user requests.
capabilitiesFixed = {'isOrdered': False,
                     'supportRoles': False,
                     'membershipIsMutable': False,
                     'metadataIsMutable': False
                    }

class Collection(namedtuple('Collection', ['id', 'pid', 'mail', 'ts'])):
    """Namedtuple representing a :class:`~Collection`.

    It includes a method to return its JSON version.
       id: collection ID (int)
       pid: uuid or pid identifying the collection
       mail: mail address of the owner
       ts: creation of the collection

    :platform: Any

    """

    __slots__ = ()

    def toJSON(self):
        """Return the JSON version of this collection.

        :returns: This collection in JSON format
        :rtype: string
        """
        # FIXME Capabilities should be later separated between (im)mutable
        interVar = ({'id': self.id,
                     'pid': self.pid,
                     'creation': self.ts,
                     'capabilities': capabilitiesFixed,
                     'properties': {'ownership': {'owner': (self.mail)},
                                    'license': '',
                                    'hasAccessRestrictions': False,
                                    'memberOf': ()
                                    }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class Member(namedtuple('Member', ['id', 'pid', 'url', 'checksum'])):
    """Namedtuple representing a :class:`~Member` of a :class:`~Collection`.

    It includes methods to to return the JSON version of the member.
       id: int identifying the member
       pid: a global PID resolvable via handle.net
       url: a URL where the data can be download
       checksum: checksum of the data to check its validity

    :platform: Any

    """

    __slots__ = ()

    def toJSON(self):
        """Return the JSON version of this collection member.

        :returns: This Member in JSON format
        :rtype: string
        """
        # FIXME Capabilities should be centralized in the top part of the file
        interVar = ({'id': self.id,
                     'pid': self.pid,
                     'url': self.url,
                     'checksum': self.checksum
                    })
        return json.dumps(interVar)


class CollJSONIter(object):
    """Iterable object capable of creating JSON chunks representing different
    types of objects. For instance, :class:`~Member` or :class:`~Collection`.

    :param cursor: MySQL cursor containing the result of a query
    :type cursor: MySQLdb.cursors.Cursor
    :param objType: Class which must be used to create the objects returned
    :type objType: type

    :platform: Any

    """

    def __init__(self, cursor, objType):
        self.cursor = cursor
        self.objType = objType
        # 0: Header must be sent; 1: Send 1st collection; 2: Send more items
        # 3: Headers have been closed and StopIteration should be raised
        self.status = 0
        self.content_type = 'application/json'

    def __iter__(self):
        return self

    def next(self):
        """Return the next object.

        Tuples are read from the cursor and returned in JSON format. A veriable
        "status" is defined to track in which state are we. Meanings are:
        0 = Header must be sent ('{"contents": [').
        1 = Send 1st element.
        2 = Send the rest of the elements.
        3 = Headers have been closed and StopIteration should be raised.

        :raises: StopIteration

        """

        # Send headers
        if self.status == 0:
            self.status = 1
            return '{"contents": ['

        # Headers have been closed. Raise StopIteration
        if self.status == 3:
            raise StopIteration

        # Load a record
        reg = self.cursor.fetchone()
        logging.debug(str(reg))
        if reg is None:
            # There are no records, close cursor and headers, set status = 3
            self.cursor.close()
            self.status = 3
            return ']}'

        JSONFactory = self.objType._make(reg)
        if self.status == 1:
            self.status = 2
            # Send first collection
            return JSONFactory.toJSON()
        else:
            # Status=2 send a separator and a collection
            return ', %s' % JSONFactory.toJSON()


class DC_Module(object):
    """Plugable module for the main :class:`~DCApp` object.

    Four methods from the Data Collections API are registered. A connection to
    the MySQL DB is established as soon as this object is created.

    :param dc: MySQL cursor containing the result of a query
    :type dc: :class:`~DCApp`
    :param confFile: File name containing the configuration file to use.
    :type confFile: string

    :platform: Any

    """

    def __init__(self, dc, confFile='../datacoll.cfg'):
        dc.registerAction('GET', ("collections",), self.collections)
        dc.registerAction('POST', ("collections",), self.collectionsPOST)
        dc.registerAction('PUT', ("collections",), self.collectionsPUT)
        dc.registerAction('GET', ("collections", "*", "capabilities"), self.capabilities)
        dc.registerAction('GET', ("collections", "*", "members"), self.members)
        dc.registerAction('POST', ("collections", "*", "members"), self.membersPOST)
        dc.registerAction('PUT', ("collections", "*", "members",), self.membersPUT)
        dc.registerAction('GET', ("collections", "*", "members", "*",
                                  "properties"), self.memberProp)

        # We keep a copy of it
        self.__dc = dc

        config = configparser.RawConfigParser()
        here = os.path.dirname(__file__)
        config.read(os.path.join(here, confFile))

        # Read connection parameters
        self.host = config.get('mysql', 'host')
        self.user = config.get('mysql', 'user')
        self.password = config.get('mysql', 'password')
        self.db = config.get('mysql', 'db')
        self.limit = config.getint('mysql', 'limit')

        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

    def memberProp(self, environ):
        """Return a property of a collection member.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: Property name and value from a collection member in JSON
            format.
        :rtype: string
        :raises: WIClientError, WINotFoundError

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cid = int(splitColl[1])
        except:
            cid = None

        try:
            mid = int(splitColl[3])
        except:
            mid = None

        try:
            prop = splitColl[5].lower()
            if not prop.isalpha():
                raise Exception()
        except:
            messDict = {'code': 0,
                        'message': 'Malformed request'
                       }
            message = json.dumps(messDict)
            raise WIClientError(message)

        # FIXME This must be the only normal replacement because the execute
        # statement do not allow to replace something as a field.
        cursor = self.conn.cursor()
        query = 'select m.%s from member as m inner join ' % prop
        query = query + 'collection as c on m.cid = c.id'

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [cid]

        whereClause.append('m.id = %s')
        sqlParams.append(mid)

        query = query + ' where ' + ' and '.join(whereClause)

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        logging.debug(str(sqlParams))
        try:
            cursor.execute(query, tuple(sqlParams))
        except:
            messDict = {'code': 0,
                        'message': 'Requested property %s not found' % prop
                       }
            message = json.dumps(messDict)
            raise WINotFoundError(message)

        result = json.dumps({prop: cursor.fetchone()})
        cursor.close()
        return result

    def members(self, environ):
        """Return a single collection member or a list of them in JSON format.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection member or a member
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WINotFoundError

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cid = int(splitColl[1])
        except:
            cid = None

        try:
            mid = int(splitColl[3])
        except:
            mid = None

        cursor = self.conn.cursor()
        query = 'select m.id, m.pid, m.url, m.checksum from member as m inner join '
        query = query + 'collection as c on m.cid = c.id '

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [cid]

        if mid is not None:
            whereClause.append('m.id = %s')
            sqlParams.append(mid)

        query = query + ' where ' + ' and '.join(whereClause)

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        logging.debug(str(sqlParams))
        cursor.execute(query, sqlParams)

        if mid is None:
            return CollJSONIter(cursor, Member)
        else:
            # Read one member because an ID is given. Check that there is
            # something to return (result set not empty)
            member = cursor.fetchone()
            cursor.close()
            if member is None:
                messDict = {'code': 0,
                            'message': 'Member ID %s or Collection ID %s not found' % (mid, cid)
                           }
                message = json.dumps(messDict)
                raise WINotFoundError(message)

            return Member._make(member).toJSON()

    def membersPUT(self, environ):
        """Update a member.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single member in JSON format.
        :rtype: string
        :raise: WIClientError

        """

        form = ''
        try:
            length = int(environ.get('CONTENT_LENGTH', '0'))
        except ValueError:
            length = 0

        # If there is a body to read
        if length != 0:
            form = environ['wsgi.input'].read(length)
        else:
            form = environ['wsgi.input'].read()

        splitColl = environ['PATH_INFO'].strip('/').split('/')

        # Read the collection ID
        try:
            cid = int(splitColl[1])
        except:
            cid = None

        # Read the member ID
        try:
            mid = int(splitColl[3])
        except:
            mid = None

        logging.debug('Text received with request:\n%s' % form)

        jsonMemb = json.loads(form)

        # Read only the fields that we support
        pid = jsonMemb.get('pid', None)
        url = jsonMemb.get('url', None)
        checksum = jsonMemb['checksum'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()

        query = 'select count(*) from member where id=%s and cid=%s'
        logging.debug(query)
        cursor.execute(query, (mid, cid))
        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()

        if (numColls[0] != 1):
            # Send Error 404
            messDict = {'code': 0,
                        'message': 'Collection (%s) or Member ID (%s) not found!' % (cid, mid)
                       }
            message = json.dumps(messDict)
            cursor.close()
            raise WINotFoundError(message)

        query = 'update member set pid=%s, url=%s, checksum=%s where cid=%s and id=%s'
        sqlParams = [pid, url, checksum, cid, mid]
        logging.debug(query)
        logging.debug(str(sqlParams))
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        query = 'select id, pid, url, checksum from member as m where cid=%s and id=%s'

        logging.debug(query)
        logging.debug(str((cid, mid)))
        cursor.execute(query, (cid, mid))

        member = cursor.fetchone()
        cursor.close()

        return Member._make(member).toJSON()

    def collectionsPUT(self, environ):
        """Update a collection.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection in JSON format.
        :rtype: string
        :raise: WIClientError

        """

        form = ''
        try:
            length = int(environ.get('CONTENT_LENGTH', '0'))
        except ValueError:
            length = 0

        # If there is a body to read
        if length != 0:
            form = environ['wsgi.input'].read(length)
        else:
            form = environ['wsgi.input'].read()


        logging.debug('Text received with request:\n%s' % form)

        jsonColl = json.loads(form)

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership']['owner'].strip()
        cid = int(jsonColl['id'])

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()
        query = 'insert into user (mail) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from user where mail=%s) limit 1'
        sqlParams = [owner, owner]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        # Read the ID from user
        query = 'select id from user where mail = %s'
        sqlParams = [owner]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # Read user ID
        uid = cursor.fetchone()[0]

        query = 'select count(*) from collection where id = %s'
        logging.debug(query)
        cursor.execute(query, (cid,))
        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()

        if (numColls[0] != 1):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection ID already exists! (%s)' % cid
                       }
            message = json.dumps(messDict)
            cursor.close()
            raise WIClientError(message)

        query = 'update collection set owner=%s, ts=DEFAULT where id=%s'
        sqlParams = [uid, cid]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        query = 'select c.id, c.pid, mail, c.ts from collection as c inner join'
        query = '%s user as u on c.owner = u.id' % query

        whereClause = list()

        whereClause.append('c.id = %s')

        if len(whereClause):
            query = '%s where %s' % (query, ' and '.join(whereClause))

        if self.limit:
            query = '%s limit %s' % (query, self.limit)

        logging.debug(query)
        cursor.execute(query, (cid,))

        coll = cursor.fetchone()
        cursor.close()

        return Collection._make(coll).toJSON()

    def membersPOST(self, environ):
        """Add a new member.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single member or a member list in
                  JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WICreated, WINotFoundError

        """

        form = ''
        try:
            length = int(environ.get('CONTENT_LENGTH', '0'))
        except ValueError:
            length = 0

        # If there is a body to read
        if length != 0:
            form = environ['wsgi.input'].read(length)
        else:
            form = environ['wsgi.input'].read()

        logging.debug('Text received with request:\n%s' % form)

        jsonColl = json.loads(form)

        splitColl = environ['PATH_INFO'].strip('/').split('/')

        # Read the collection ID
        try:
            cid = int(splitColl[1])
        except:
            cid = None

        # Read only the fields that we support
        pid = jsonColl.get('pid', None)
        url = jsonColl.get('url', None)
        checksum = jsonColl['checksum'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()

        query = 'select count(*) from collection where id = %s'
        sqlParams = [cid]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()
        logging.debug('Collections found: %s' % numColls)

        if ((type(numColls) != tuple) or (numColls[0] != 1)):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection ID could not be found! (%s)' % cid
                       }
            message = json.dumps(messDict)
            cursor.close()
            raise WINotFoundError(message)

        query = 'insert into member (cid, pid, url, checksum) values (%s, %s, %s, %s)'
        sqlParams = [cid, pid, url, checksum]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()
        cursor.close()
        raise WICreated('Member %s added' % (pid if pid is not None else url))

    def collectionsPOST(self, environ):
        """Create a new collection.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection or a collection
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WICreated, WIClientError

        """

        form = ''
        try:
            length = int(environ.get('CONTENT_LENGTH', '0'))
        except ValueError:
            length = 0

        # If there is a body to read
        if length != 0:
            form = environ['wsgi.input'].read(length)
        else:
            form = environ['wsgi.input'].read()


        logging.debug('Text received with request:\n%s' % form)

        jsonColl = json.loads(form)

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership']['owner'].strip()
        pid = jsonColl['pid'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()
        query = 'insert into user (mail) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from user where mail=%s) limit 1'
        sqlParams = [owner, owner]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        # Read the ID from user
        query = 'select id from user where mail = %s'
        sqlParams = [owner]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # Read user ID
        uid = cursor.fetchone()[0]

        query = 'select count(*) from collection where pid = %s'
        sqlParams = [pid]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()

        if ((type(numColls) != tuple) or numColls[0]):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection PID already exists! (%s)' % pid
                       }
            message = json.dumps(messDict)
            cursor.close()
            raise WIClientError(message)

        query = 'insert into collection (pid, owner) values (%s, %s)'
        sqlParams = [pid, uid]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()
        cursor.close()
        raise WICreated('Collection %s created' % str(pid))

    def collections(self, environ):
        """Return a single/list of collection(s).

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection or a collection
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WINotFoundError

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        owner = form.getfirst('filter_by_owner', None)

        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            # Read collection ID
            cid = int(splitColl[1])
        except:
            cid = None

        cursor = self.conn.cursor()
        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id'

        whereClause = list()
        sqlParams = list()

        # Filter by owner if present in the parameters
        if owner is not None:
            whereClause.append('u.mail = %s')
            sqlParams.append(owner)

        # Show only one collection if ID is present
        if cid is not None:
            whereClause.append('c.id = %s')
            sqlParams.append(cid)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        if cid is None:
            # If no ID is given iterate through all collections in cursor
            return CollJSONIter(cursor, Collection)
        else:
            # Read one collection because an ID is given. Check that there is
            # something to return (result set not empty)
            coll = cursor.fetchone()
            cursor.close()
            if coll is None:
                messDict = {'code': 0,
                            'message': 'Collection ID %s not found' % cid
                           }
                message = json.dumps(messDict)
                raise WINotFoundError(message)

            return Collection._make(coll).toJSON()

    def capabilities(self, environ):
        """Return the capabilities of a collection.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: The capabilities of a collection in JSON format.
        :rtype: string
        :raise: WINotFoundError

        """

        # For the time being, this are fixed collections.
        # To be modified in the future with mutable collections
        splitColl = environ['PATH_INFO'].strip('/').split('/')
        # It should be clear that the first component is "collections" and the
        # third one is "capabilities". The second is the ID and should be read.
        cid = int(splitColl[1])

        cursor = self.conn.cursor()
        query = 'select id from collection where id = %s'

        logging.debug(query)
        cursor.execute(query, (cid,))

        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % cid
                       }
            message = json.dumps(messDict)
            raise WINotFoundError(message)

        return json.dumps(capabilitiesFixed)
