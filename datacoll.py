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
import configparser
import logging
import os
import json

# For the time being these are the capabilities for the immutable datasets
# coming from the user requests.
capabilitiesFixed = {'isOrdered': False,
                     'supportRoles': False,
                     'membershipIsMutable': False,
                     'metadataIsMutable': False
                    }


class MemberAPI(object):
    @cherrypy.expose
    def GET(self, collID, memberID):
        return "member.GET(%d, %d)" % (int(collID), int(memberID))

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

        splitColl = environ['PATH_INFO'].strip('/').split('/')

        # Read only the fields that we support
        pid = jsonColl.get('pid', None)
        url = jsonColl.get('url', None)
        checksum = jsonColl['checksum'].strip()

        # Insert only if the user does not exist yet
        cursor = self.conn.cursor()

        query = 'select count(*) from collection where id = %s'
        sqlParams = [collID]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # FIXME Check the type of numColls!
        numColls = cursor.fetchone()
        logging.debug('Collections found: %s' % numColls)

        if ((type(numColls) != tuple) or (numColls[0] != 1)):
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection ID could not be found! (%s)' % collID
                       }
            message = json.dumps(messDict)
            cursor.close()
            raise cherrypy.HTTPError(404, message)

        query = 'insert into member (cid, pid, url, checksum) values (%s, %s, %s, %s)'
        sqlParams = [cid, pid, url, checksum]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()
        cursor.close()
        raise cherrypy.HTTPError(201, 'Member %s added' % (pid if pid is not None else url))


class MembersAPI(object):
    @cherrypy.expose
    def GET(self, collID):
        return "members.GET(%d)" % int(collID)


class CollectionsAPI(object):
    # FIXME ! It is still not clear why here the handler looks for "index" and
    # not "GET".
    @cherrypy.expose(['index'])
    def GET(self, owner=None):
        """Return a list of collection(s).

        :returns: An iterable object with a single collection or a collection
            list in JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WINotFoundError

        """

        cursor = self.conn.cursor()
        query = 'select c.id, c.pid, mail, ts from collection as c inner join '
        query = query + 'user as u on c.owner = u.id'

        whereClause = list()
        sqlParams = list()

        # Filter by owner if present in the parameters
        if owner is not None:
            whereClause.append('u.mail = %s')
            sqlParams.append(owner)

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # If no ID is given iterate through all collections in cursor
        return CollJSONIter(cursor, Collection)


class CollectionAPI(object):
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

        logging.debug(query)
        cursor.execute(query, (collID,))

        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID
                       }
            message = json.dumps(messDict)

            raise cherrypy.HTTPError(404, message)

        return json.dumps(capabilitiesFixed)

    @cherrypy.expose
    def POST(self):
        """Create a new collection.

        :returns: An iterable object with a single collection or a collection
            list in JSON format.
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
            raise cherrypy.HTTPError(400, message)

        query = 'insert into collection (pid, owner) values (%s, %s)'
        sqlParams = [pid, uid]
        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))
        self.conn.commit()
        cursor.close()
        raise cherrypy.HTTPError(202, 'Collection %s created' % str(pid))

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
            raise cherrypy.HTTPError(400, message)

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
        cursor.execute(query, (collID,))

        coll = cursor.fetchone()
        cursor.close()

        return Collection._make(coll).toJSON()


    @cherrypy.expose
    def GET(self, collID):
        """Return a single collection.

        :returns: An iterable object with a single collection in JSON format.
        :rtype: string or :class:`~CollJSONIter`
        :raise: WINotFoundError

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

        if self.limit:
            query = query + ' limit %s'
            sqlParams.append(self.limit)

        logging.debug(query)
        cursor.execute(query, tuple(sqlParams))

        # Read one collection because an ID is given. Check that there is
        # something to return (result set not empty)
        coll = cursor.fetchone()
        cursor.close()
        if coll is None:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID
                       }
            message = json.dumps(messDict)
            raise WINotFoundError(message)

        return Collection._make(coll).toJSON()


class DataColl(object):
    def __init__(self):
        self.coll = CollectionAPI()
        self.members = MembersAPI()
        self.member = MemberAPI()
        self.colls = CollectionsAPI()

    def _cp_dispatch(self, vpath):
        queryStr = '/'.join(vpath)

        if len(vpath):
            if vpath[0] == "features":
                return self

            if vpath[0] == "collections":
                # Replace "collections" with the request method (e.g. GET, POST)
                vpath[0] = cherrypy.request.method

                # If there are no more terms to process
                if len(vpath) < 2:
                    print vpath
                    return self.colls

                cherrypy.request.params['collID'] = vpath.pop(1)
                if len(vpath) > 1:
                    # Remove a word and check that is "members"
                    auxTerm = vpath.pop(1)
                    if auxTerm not in ("members", "capabilities"):
                        raise cherrypy.HTTPError(400, 'Bad Request')

                    if auxTerm == "capabilities":
                        return self.coll

                    if len(vpath) > 1:
                        cherrypy.request.params['memberID'] = vpath.pop(1)
                        return self.member

                    return self.members

                print vpath
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
