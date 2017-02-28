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

##################################################################
#
# First all the imports
#
##################################################################


import cherrypy
import logging
import os
import json
import MySQLdb
from dcmysql import Collection
from dcmysql import Member
from dcmysql import CollJSONIter

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

# FIXME This is hardcoded but should be read from the configuration file
limit = 100
# For the time being these are the capabilities for the immutable datasets
# coming from the user requests.
capabilitiesFixed = {'isOrdered': False,
                     'supportRoles': False,
                     'membershipIsMutable': False,
                     'metadataIsMutable': False}


class MemberAPI(object):
    def __init__(self, conn):
        self.conn = conn

    @cherrypy.expose
    def GET(self, collID, memberID):
        """Return a single collection member in JSON format.

        :returns: An iterable object with a single collection member in JSON
                  format.
        :rtype: string or :class:`~CollJSONIter`

        """
        cursor = self.conn.cursor()
        query = 'select m.id, m.pid, m.url, m.checksum from member as m inner '
        query = query + 'join collection as c on m.cid = c.id '

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [collID]

        whereClause.append('m.id = %s')
        sqlParams.append(memberID)

        query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        cursor.execute(query, sqlParams)

        # Read one member because an ID is given. Check that there is
        # something to return (result set not empty)
        member = cursor.fetchone()
        cursor.close()
        if member is None:
            messDict = {'code': 0,
                        'message': 'Member %s or Collection %s not found'
                        % (memberID, collID)}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        return Member._make(member).toJSON()


class MembersAPI(object):
    def __init__(self, conn):
        self.conn = conn

    @cherrypy.expose
    def GET(self, collID):
        """Return a list of collection members in JSON format.

        :returns: An iterable object with a member list in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """
        cursor = self.conn.cursor()
        query = 'select id from collection where id = %s'

        cursor.execute(query, (collID,))

        coll = cursor.fetchone()
        cursor.close()

        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)

            raise cherrypy.HTTPError(404, message)

        cursor = self.conn.cursor()
        query = 'select m.id, m.pid, m.url, m.checksum from member as m inner '
        query = query + 'join collection as c on m.cid = c.id '

        whereClause = list()
        whereClause.append('c.id = %s')
        sqlParams = [collID]

        query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        cursor.execute(query, sqlParams)

        return CollJSONIter(cursor, Member)

    def POST(self, collID):
        """Add a new member.

        :returns: An iterable object with a single member or a member list in
                  JSON format.
        :rtype: string or :class:`~CollJSONIter`

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
        pid = jsonColl.get('pid', None)
        url = jsonColl.get('url', None)
        checksum = jsonColl['checksum'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()

        query = 'select count(*) from collection where id = %s'
        sqlParams = [collID]

        cursor.execute(query, tuple(sqlParams))

        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()
        logging.debug('Collections found: %s' % numColls)

        if ((type(numColls) != tuple) or (numColls[0] != 1)):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection ID could not be found! (%s)'
                        % collID}
            message = json.dumps(messDict)
            cursor.close()
            raise cherrypy.HTTPError(404, message)

        query = 'insert into member (cid, pid, url, checksum) values ' + \
            '(%s, %s, %s, %s)'
        sqlParams = [collID, pid, url, checksum]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()
        cursor.close()
        raise cherrypy.HTTPError(201, 'Member %s added' %
                                 (pid if pid is not None else url))


class CollectionsAPI(object):
    def __init__(self, conn):
        self.conn = conn

    # FIXME ! It is still not clear why here the handler looks for "index" and
    # not "GET".
    # @cherrypy.expose(['index'])

    @cherrypy.expose
    def default(self, **kwargs):
        # print kwargs

        toCall = getattr(self, cherrypy.request.method)
        return toCall()

    @cherrypy.expose
    def GET(self, filter_by_owner=None):
        """Return a list of collections.

        :returns: An iterable object with a collection list in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """
        # print 'bodyGET', cherrypy.request.body

        cursor = self.conn.cursor()
        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id'

        whereClause = list()
        sqlParams = list()

        # Filter by owner if present in the parameters
        if filter_by_owner is not None:
            whereClause.append('u.mail = %s')
            sqlParams.append(filter_by_owner)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        cursor.execute(query, tuple(sqlParams))

        # If no ID is given iterate through all collections in cursor
        return CollJSONIter(cursor, Collection)

    @cherrypy.expose
    def POST(self):
        """Create a new collection.

        :returns: An iterable object with a single collection or a collection
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """
        
        jsonColl = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership'].strip()
        pid = jsonColl['pid'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()
        query = 'insert into user (mail) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from user where mail=%s) '
        query = query + 'limit 1'
        sqlParams = [owner, owner]

        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        # Read the ID from user
        query = 'select id from user where mail = %s'
        sqlParams = [owner]

        cursor.execute(query, tuple(sqlParams))

        # Read user ID
        uid = cursor.fetchone()[0]

        query = 'select count(*) from collection where pid = %s'
        sqlParams = [pid]
        cursor.execute(query, tuple(sqlParams))

        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()

        if ((type(numColls) != tuple) or numColls[0]):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection PID already exists! (%s)' % pid}
            message = json.dumps(messDict)
            cursor.close()
            raise cherrypy.HTTPError(400, message)

        query = 'insert into collection (pid, owner) values (%s, %s)'
        sqlParams = [pid, uid]
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id where c.pid = %s'
        cursor.execute(query, (pid,))

        coll = cursor.fetchone()

        cursor.close()

        cherrypy.response.status = '201 Collection %s created' % str(pid)
        cherrypy.response.header_list = [('Content-Type', 'application/json')]
        return Collection._make(coll).toJSON()


class CollectionAPI(object):
    def __init__(self, conn):
        self.conn = conn

    @cherrypy.expose
    def capabilities(self, collID):
        """Return the capabilities of a collection.

        :returns: The capabilities of a collection in JSON format.
        :rtype: string

        """
        # For the time being, these are fixed collections.
        # To be modified in the future with mutable collections
        cursor = self.conn.cursor()
        query = 'select id from collection where id = %s'

        cursor.execute(query, (collID,))

        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)

            raise cherrypy.HTTPError(404, message)

        return json.dumps(capabilitiesFixed)

    @cherrypy.expose
    def PUT(self, collID):
        """Update a collection.

        :returns: An iterable object with a single collection in JSON format.
        :rtype: string

        """
        # If there is a body to read
        if length != 0:
            form = environ['wsgi.input'].read(length)
        else:
            form = environ['wsgi.input'].read()

        logging.debug('Text received with request:\n%s' % form)

        jsonColl = json.loads(form)

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership']['owner'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()
        query = 'insert into user (mail) select * from (select %s) as tmp '
        query = query + 'where not exists (select id from user where mail=%s) '
        query = query + 'limit 1'
        sqlParams = [owner, owner]

        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        # Read the ID from user
        query = 'select id from user where mail = %s'
        sqlParams = [owner]

        cursor.execute(query, tuple(sqlParams))

        # Read user ID
        uid = cursor.fetchone()[0]

        query = 'select count(*) from collection where id = %s'

        cursor.execute(query, (collID,))
        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()

        if (numColls[0] != 1):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection ID not found! (%s)' % collID}
            message = json.dumps(messDict)
            cursor.close()
            raise cherrypy.HTTPError(404, message)

        query = 'update collection set owner=%s, ts=DEFAULT where id=%s'
        sqlParams = [uid, collID]

        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()

        query = 'select c.id, c.pid, mail, c.ts from collection as c inner '
        query = query + 'join user as u on c.owner = u.id'

        whereClause = list()

        whereClause.append('c.id = %s')

        if len(whereClause):
            query = '%s where %s' % (query, ' and '.join(whereClause))

        if limit:
            query = '%s limit %s' % (query, limit)

        cursor.execute(query, (collID,))

        coll = cursor.fetchone()
        cursor.close()

        return Collection._make(coll).toJSON()

    @cherrypy.expose
    def DELETE(self, collID):
        """Delete a single collection."""
        cursor = self.conn.cursor()
        query = 'select count(id) from collection'

        whereClause = list()
        sqlParams = list()

        # Show only one collection if ID is present
        whereClause.append('id = %s')
        sqlParams.append(collID)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        cursor.execute(query, tuple(sqlParams))

        # Read how many collections I found. It should be 1 or 0.
        numb = cursor.fetchone()
        if numb[0] == 1:
            query = 'delete from collection where id = %s'
            cursor.execute(query, (collID, ))
            cursor.close()
            self.conn.commit()

        elif numb[0] == 0:
            cursor.close()
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        else:
            cursor.close()
            messDict = {'code': 0,
                        'message': 'Wrong data! Two or more records found with a key (%s)' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        return ""

    @cherrypy.expose
    def GET(self, collID):
        """Return a single collection.

        :returns: An iterable object with a single collection in JSON format.
        :rtype: string or :class:`~CollJSONIter`

        """
        cursor = self.conn.cursor()
        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id'

        whereClause = list()
        sqlParams = list()

        # Show only one collection if ID is present
        whereClause.append('c.id = %s')
        sqlParams.append(collID)

        if len(whereClause):
            query = query + ' where ' + ' and '.join(whereClause)

        if limit:
            query = query + ' limit %s'
            sqlParams.append(limit)

        cursor.execute(query, tuple(sqlParams))

        # Read one collection because an ID is given. Check that there is
        # something to return (result set not empty)
        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        return Collection._make(coll).toJSON()


class DataColl(object):
    def __init__(self):
        config = configparser.RawConfigParser()
        here = os.path.dirname(__file__)
        config.read(os.path.join(here, 'datacoll.cfg'))

        # Read connection parameters
        self.host = config.get('mysql', 'host')
        self.user = config.get('mysql', 'user')
        self.password = config.get('mysql', 'password')
        self.db = config.get('mysql', 'db')
        limit = config.getint('mysql', 'limit')

        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

        self.coll = CollectionAPI(self.conn)
        self.members = MembersAPI(self.conn)
        self.member = MemberAPI(self.conn)
        self.colls = CollectionsAPI(self.conn)

    def _cp_dispatch(self, vpath):
        if len(vpath):
            if vpath[0] == "features":
                return self

            if vpath[0] == "collections":
                # Replace "collections" with the request method (e.g. GET, PUT)
                vpath[0] = cherrypy.request.method

                # If there are no more terms to process
                if len(vpath) < 2:
                    return self.colls

                # Remove the collection ID
                cherrypy.request.params['collID'] = vpath.pop(1)
                if len(vpath) > 1:
                    # Remove a word and check that is "members"
                    if vpath[1] not in ("members", "capabilities"):
                        raise cherrypy.HTTPError(400, 'Bad Request')

                    if vpath[1] == "capabilities":
                        vpath.pop(0)
                        return self.coll

                    if vpath[1] == "members":
                        # Remove "members"
                        vpath.pop(1)

                        # Check if there are more parameters
                        if len(vpath) > 1:
                            cherrypy.request.params['memberID'] = vpath.pop(1)
                            return self.member

                        return self.members

                return self.coll

        return vpath

    @cherrypy.expose
    def features(self):
        """Read the features of the system and return them in JSON format.

        :returns: System capabilities in JSON format
        :rtype: string

        """
        syscapab = {"pidProviderType": "",
                    "enforcesAccess": False,
                    "supportsPagination": False,
                    "ruleBasedGeneration": False,
                    "maxExpansionDepth": 0}
        return json.dumps(syscapab)


if __name__ == "__main__":
    cherrypy.quickstart(DataColl(), '/rda/datacoll')
