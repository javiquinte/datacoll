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
import os
import json
import MySQLdb
from dcmysql import Collection
from dcmysql import Collections
from dcmysql import Member
from dcmysql import Members
from dcmysql import JSONFactory
from dcmysql import urlFile

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

version = '0.1b1'

# FIXME This is hardcoded but should be read from the configuration file
limit = 100
# For the time being these are the capabilities for the immutable datasets
# coming from the user requests.
capabilitiesFixed = {
                      "isOrdered": False,
                      "appendsToEnd": True,
                      "supportsRoles": False,
                      "membershipIsMutable": True,
                      "metadataIsMutable": True,
                      "restrictedToType": "string",
                      "maxLength": -1
                    }


class MemberAPI(object):
    """Object dispatching methods related to a single Member."""

    def __init__(self, conn):
        """Constructor of the MemberAPI class.

:param conn: Connection to the MySQL DB.
:type conn: MySQLdb.connections.Connection
        """
        self.conn = conn

    @cherrypy.expose
    def DELETE(self, collID, memberID):
        """Delete a single member from a collection.

:param collID: Collection ID.
:type collID: int
:param memberID: Member ID.
:type memberID: int
:returns: Empty string
:rtype: string
:raises: cherrypy.HTTPError
"""
        try:
            member = Member(self.conn, collID=collID, id=memberID)
        except:
            msg = 'Member ID %s within collection ID %s not found'
            messDict = {'code': 0,
                        'message': msg % (memberID, collID)}
            message = json.dumps(messDict)

            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        member.delete(self.conn)

        return ""

    @cherrypy.expose
    def download(self, collID, memberID):
        """Download a single collection member in JSON format.

:param collID: Collection ID.
:type collID: int
:param memberID: Member ID.
:type memberID: int
:returns: An iterable object which downloads the member of a collection.
:rtype: :class:`~urlFile`
:raises: cherrypy.HTTPError
"""
        try:
            member = Member(self.conn, collID=collID, id=memberID)
        except:
            messDict = {'code': 0,
                        'message': 'Member %s or Collection %s not found'
                        % (memberID, collID)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # If the user wants to download the resource pointed by the member
        if member.pid is not None:
            url = 'http://hdl.handle.net/%s' % member.pid
        else:
            url = member.location

        cherrypy.response.headers['Content-Type'] = 'application/octet-stream'
        return urlFile(url)

    download._cp_config = {'response.stream': True}

    @cherrypy.expose
    def GET(self, collID, memberID):
        """Return a single collection member in JSON format.

:param collID: Collection ID.
:type collID: int
:param memberID: Member ID.
:type memberID: int
:returns: Metadata related to a member of a collection in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        try:
            member = Member(self.conn, collID=collID, id=memberID)
        except:
            messDict = {'code': 0,
                        'message': 'Member %s or Collection %s not found'
                        % (memberID, collID)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return member.toJSON()

    @cherrypy.expose
    def PUT(self, collID, memberID):
        """Update an existing member.

:param collID: Collection ID.
:type collID: int
:param memberID: Member ID.
:type memberID: int
:returns: Metadata related to the updated member in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        pid = jsonMemb.get('pid', None)
        location = jsonMemb.get('location', None)
        checksum = jsonMemb.get('checksum', None)
        datatype = jsonMemb.get('datatype', None)
        index = jsonMemb.get('mappings', {}).get('index', None)

        try:
            member = Member(self.conn, collID=collID, id=memberID)
        except:
            # FIXME We need to check here the datatype by querying the
            # collection and comparing with the restrictedToType attribute
            # Send Error 404
            msg = 'Member %s from Collection %s not found!'
            messDict = {'code': 0,
                        'message': msg % (memberID, collID)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # Check that the index does not collide with existent IDs
        if index != memberID:
            try:
                Member(self.conn, collID=collID, id=index)
                # Send Error 400
                msg = 'Index %s is already used for Collection %s !'
                messDict = {'code': 0,
                            'message': msg % (index, collID)}
                message = json.dumps(messDict)
                cherrypy.response.headers['Content-Type'] = 'application/json'
                raise cherrypy.HTTPError(400, message)
            except:
                pass

        member.update(self.conn, pid=pid, location=location, checksum=checksum,
                      datatype=datatype, id=index)

        # Read the member
        try:
            memb = Member(self.conn, collID=collID, id=index)
        except:
            # Send Error 400
            msg = 'Member seems not to be properly saved.'
            messDict = {'code': 0,
                        'message': msg}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return memb.toJSON()


class MembersAPI(object):
    """Object dispatching methods related to a list of  Members."""

    def __init__(self, conn):
        """Constructor of a list of Members."""
        self.conn = conn

    @cherrypy.expose
    def GET(self, collID):
        """Return a list of collection members in JSON format.

:param collID: Collection ID.
:type collID: int
:returns: Metadata related to all members of a collection in JSON format.
:rtype: string
"""
        membList = Members(self.conn, collID=collID)

        # If no ID is given iterate through all collections in cursor
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return JSONFactory(membList, Member)

    @cherrypy.expose
    def POST(self, collID):
        """Add a new member to an existing collection.

:param collID: Collection ID.
:type collID: int
:returns: Metadata related to the new member in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        pid = jsonMemb.get('pid', None)
        location = jsonMemb.get('location', None)
        datatype = jsonMemb.get('datatype', None)
        checksum = jsonMemb.get('checksum', None)
        index = jsonMemb.get('mappings', {}).get('index', None)

        try:
            Collection(self.conn, collID=collID)
        except:
            # Send Error 404
            messDict = {'code': 0,
                        'message': 'Collection %s not found!' % collID}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # FIXME Here we need to set also the datatype after checking the
        # restrictedToType attribute in the collection
        try:
            member = Member(None).insert(self.conn, collID=collID, pid=pid,
                                         location=location)
        except:
            msg = 'Member not properly saved. Error when querying it.'
            messDict = {'code': 0,
                        'message': msg}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, msg)

        cherrypy.response.status = '201 Member created (%s)' % \
            (pid if pid is not None else location)
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return member.toJSON()


class CollectionsAPI(object):
    """Object dispatching methods related to a list of Collections."""

    def __init__(self, conn):
        """Constructor of the CollectionsAPI."""
        self.conn = conn

    # FIXME ! It is still not clear why here the handler looks for "index" and
    # not "GET".
    # @cherrypy.expose(['index'])

    @cherrypy.expose
    def default(self, **kwargs):
        """Default method when there is no match."""
        # print kwargs

        toCall = getattr(self, cherrypy.request.method)
        return toCall()

    @cherrypy.expose
    def GET(self, filter_by_owner=None):
        """Return a list of collections.

:param filter_by_owner: Mail from the owner of the collection.
:type filter_by_owner: string
:returns: Metadata related to all collections in JSON format.
:rtype: string
"""
        coll = Collections(self.conn, owner=filter_by_owner)

        # If no ID is given iterate through all collections in cursor
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return JSONFactory(coll, Collection)

    @cherrypy.expose
    def POST(self):
        """Create a new collection.

:returns: Metadata related to the new collection in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        jsonColl = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership'].strip()
        pid = jsonColl['pid'].strip()

        try:
            coll = Collection(self.conn, pid=pid)

            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection PID already exists! (%s)' % pid}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)
        except:
            pass

        try:
            # It is important to call insert inline with an empty Collection!
            coll = Collection(None).insert(self.conn, owner=owner, pid=pid)
        except:
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection could not be inserted'}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        cherrypy.response.status = '201 Collection %s created' % str(pid)
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.toJSON()


class CollectionAPI(object):
    """Object dispatching methods related to a single Collection."""

    def __init__(self, conn):
        """Constructor of the CollectionAPI."""
        self.conn = conn

    @cherrypy.expose
    def download(self, collID):
        """Download a complete collection.

:param collID: Collection ID.
:type collID: int
:returns: Contents of the collection concatenated.
:rtype: bytestream
"""
        members = Members(self.conn, collID=collID)

        # Read one member because an ID is given. Check that there is
        # something to return (result set not empty)
        member = members.fetchone()
        if member:
            cherrypy.response.headers['Content-Type'] = 'application/octet-stream'
        while member:
            # If the user wants to download the resource pointed by the member
            if member.pid is not None:
                url = 'http://hdl.handle.net/%s' % member.pid
            else:
                url = member.location

            for buf in urlFile(url):
                yield buf
            member = members.fetchone()

    download._cp_config = {'response.stream': True}

    @cherrypy.expose
    def capabilities(self, collID):
        """Return the capabilities of a collection.

:param collID: Collection ID.
:type collID: int
:returns: The capabilities of the collection in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        # For the time being, these are fixed collections.
        # To be modified in the future with mutable collections
        try:
            coll = Collection(self.conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps(capabilitiesFixed)

    @cherrypy.expose
    def PUT(self, collID):
        """Update an existing collection.

:param collID: Collection ID.
:type collID: int
:returns: Metadata related to the updated collection in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        jsonColl = json.loads(cherrypy.request.body.fp.read())

        try:
            coll = Collection(self.conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        # cursor = self.conn.cursor()

        # Read only the fields that we support
        owner = jsonColl['properties']['ownership'].strip()
        pid = jsonColl['pid'].strip()

        # FIXME I must check if the object coll is being updated as in the DB!
        coll.update(owner=owner, pid=pid)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.toJSON()

    @cherrypy.expose
    def DELETE(self, collID):
        """Delete a single collection.

:param collID: Collection ID.
:type collID: int
:returns: Empty string.
:rtype: string
:raises: cherrypy.HTTPError
"""
        try:
            coll = Collection(self.conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        coll.delete(self.conn)

        return ""

    @cherrypy.expose
    def GET(self, collID):
        """Return a single collection.

:param collID: Collection ID.
:type collID: int
:returns: Metadata related to the collection in JSON format.
:rtype: string
:raises: cherrypy.HTTPError
"""
        try:
            coll = Collection(self.conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.toJSON()


class DataColl(object):
    """Main class with the dispatcher and the connection to the DB."""

    def __init__(self):
        """Constructor of the DataColl object."""
        config = configparser.RawConfigParser()
        here = os.path.dirname(__file__)
        config.read(os.path.join(here, 'datacoll.cfg'))

        # Read connection parameters
        self.host = config.get('mysql', 'host')
        self.user = config.get('mysql', 'user')
        self.password = config.get('mysql', 'password')
        self.db = config.get('mysql', 'db')
        self.limit = config.getint('mysql', 'limit')

        self.conn = MySQLdb.connect(self.host, self.user, self.password,
                                    self.db)

        self.coll = CollectionAPI(self.conn)
        self.members = MembersAPI(self.conn)
        self.member = MemberAPI(self.conn)
        self.colls = CollectionsAPI(self.conn)

    def _cp_dispatch(self, vpath):
        if len(vpath):
            if vpath[0] in ("features", "version"):
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
                    if vpath[1] not in ("members", "capabilities", "download"):
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
                            if (len(vpath) > 1) and (vpath[1] == "download"):
                                vpath.pop(0)
                            return self.member

                        return self.members

                    if vpath[1] == "download":
                        vpath.pop(0)
                        return self.coll

                return self.coll

        return vpath

    @cherrypy.expose
    def features(self):
        """Read the features of the system and return them in JSON format.

:returns: System capabilities in JSON format
:rtype: string
"""
        syscapab = {
                     "providesCollectionPids": False,
                     "collectionPidProviderType": "string",
                     "enforcesAccess": False,
                     "supportsPagination": False,
                     "asynchronousActions": False,
                     "ruleBasedGeneration": True,
                     "maxExpansionDepth": 4,
                     "providesVersioning": False,
                     "supportedCollectionOperations": [],
                     "supportedModelTypes": []
                   }
        cherrypy.response.header_list = [('Content-Type', 'application/json')]
        return json.dumps(syscapab)

    @cherrypy.expose
    def version(self):
        """Return the version of this implementation.

:returns: System capabilities in JSON format
:rtype: string
"""
        cherrypy.response.header_list = [('Content-Type', 'text/plain')]
        return version


if __name__ == "__main__":
    cherrypy.quickstart(DataColl(), '/rda/datacoll')
