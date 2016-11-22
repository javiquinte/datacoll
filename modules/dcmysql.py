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

# For the time being these are the capabilities for the immutable datasets
# coming from the user requests.
capabilitiesFixed = {'isOrdered': False,
                     'supportRoles': False,
                     'membershipIsMutable': False,
                     'metadataIsMutable': False
                    }

class Collection(namedtuple('Collection', ['id', 'mail', 'ts'])):
    """Namedtuple representing a :class:`~Collection`.

    It includes a method to return its JSON version.
       id: uuid or pid identifying the collection
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
                     'creation': self.ts,
                     'capabilities': capabilitiesFixed,
                     'properties': {'ownership': {'owner': (self.mail)},
                                    'license': '',
                                    'hasAccessRestrictions': False,
                                    'memberOf': ()
                                    }
                    })
        return json.dumps(interVar, default=datetime.datetime.isoformat)


class Member(namedtuple('Member', ['id', 'checksum'])):
    """Namedtuple representing a :class:`~Member` of a :class:`~Collection`.

    It includes methods to to return the JSON version of the member.
       id: uuid or pid identifying the member
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
        if reg is None:
            # There are no records, close cursor and headers, set status = 3
            self.cursor.close()
            self.status = 3
            return ']}'

        JSONFactory = self.objType._make(reg)
        if self.status == 1:
            # Send first collection
            return JSONFactory.toJSON()
        else:
            # Send a separator and a collection
            return ', %s' % reg.toJSON()


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
        dc.registerAction(("collections",), self.collections)
        dc.registerAction(("collections", "*", "capabilities"),
                          self.capabilities)
        dc.registerAction(("collections", "*", "members"), self.members)
        dc.registerAction(("collections", "*", "members", "*", "properties"),
                          self.memberProp)

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
        self.limit = config.get('mysql', 'limit')

        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

    def memberProp(self, environ):
        """Return a property of a collection member.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: Property name and value from a collection member in JSON
            format.
        :rtype: string
        :raises: Exception

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cpid = splitColl[1]
        except:
            cpid = None

        try:
            mpid = splitColl[3]
        except:
            mpid = None

        try:
            prop = splitColl[5]
        except:
            raise Exception('A name of a property should be given.')

        # FIXME All parameters MUST be checked to avoid unwanted SQL injection!
        cursor = self.conn.cursor()
        query = 'select m.%s from member as m inner join' % prop.lower()
        query = '%s collection as c on m.cid = c.id' % query

        whereClause = list()
        whereClause.append('c.pid = "%s"' % cpid)

        if mpid is not None:
            whereClause.append('m.pid = "%s"' % mpid)

        query = '%s where %s' % (query, ' and '.join(whereClause))

        if self.limit:
            query = '%s limit %s' % (query, self.limit)

        logging.debug(query)
        try:
            cursor.execute(query)
        except:
            raise Exception('Empty result set with the given parameters')

        return json.dumps({prop: cursor.fetchone()})


    def members(self, environ):
        """Return a single collection member or a list of them in JSON format.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection member or a member
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cpid = splitColl[1]
        except:
            cpid = None

        try:
            mpid = splitColl[3]
        except:
            mpid = None

        cursor = self.conn.cursor()
        query = 'select m.pid, m.checksum from member as m inner join'
        query = '%s collection as c on m.cid = c.id' % query

        whereClause = list()
        whereClause.append('c.pid = "%s"' % cpid)

        if mpid is not None:
            whereClause.append('m.pid = "%s"' % mpid)

        query = '%s where %s' % (query, ' and '.join(whereClause))

        if self.limit:
            query = '%s limit %s' % (query, self.limit)

        logging.debug(query)
        cursor.execute(query)

        if mpid is None:
            return CollJSONIter(cursor, Member)
        else:
            # FIXME Empty set not considered!
            return Member._make(cursor.fetchone()).toJSON()


    def collections(self, environ):
        """Return a single/list of collection(s).

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: An iterable object with a single collection or a collection
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """

        # The keep_blank_values=1 is needed to recognize the download key despite
        # that it has no value associated (e.g. api/registered/fullpath?download)
        form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ,
                                keep_blank_values=1)

        logging.debug('Parameters received: %s' % form.keys())
        owner = form.getfirst('filter_by_owner', None)

        splitColl = environ['PATH_INFO'].strip('/').split('/')

        try:
            cpid = splitColl[1]
        except:
            cpid = None

        cursor = self.conn.cursor()
        query = 'select pid, mail, ts from collection as c inner join'
        query = '%s user as u on c.owner = u.id' % query

        whereClause = list()

        if owner is not None:
            whereClause.append('u.mail = "%s"' % owner)

        if cpid is not None:
            whereClause.append('c.pid = "%s"' % cpid)

        if len(whereClause):
            query = '%s where %s' % (query, ' and '.join(whereClause))

        if self.limit:
            query = '%s limit %s' % (query, self.limit)

        logging.debug(query)
        cursor.execute(query)

        if cpid is None:
            return CollJSONIter(cursor, Collection)
        else:
            # FIXME Empty set not considered!
            return Collection._make(cursor.fetchone()).toJSON()

    def capabilities(self, environ):
        """Return the capabilities of a collection.

        :param environ: Environment as provided by the Apache WSGI module
        :type environ: dict ?
        :returns: The capabilities of a collection in JSON format.
        :rtype: string

        """

        # For the time being, this are fixed collections.
        # To be modified in the future with mutable collections
        splitColl = environ['PATH_INFO'].split('/')
        # It should be clear that the first component is "collections" and the
        # third one is "capabilities". The second is the ID and should br read.
        # FIXME This is useless for the moment.
        cid = splitColl[1]
        return json.dumps(capabilitiesFixed)
