#!/usr/bin/env python3
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
import configparser
import MySQLdb
from dcmysql import Collection
from dcmysql import Collections
from dcmysql import Member
from dcmysql import Members
from dcmysql import JSONFactory
from dcmysql import urlFile

version = '0.2a1.dev1'
cfgfile = 'datacoll.cfg'

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
                      "maxLength": -1,
                      "ruleBasedGeneration": True
                    }

"""Constructor of the DataColl object."""
config = configparser.RawConfigParser()
here = os.path.dirname(__file__)
config.read(os.path.join(here, cfgfile))

# Read connection parameters
host = config.get('mysql', 'host')
user = config.get('mysql', 'user')
password = config.get('mysql', 'password')
db = config.get('mysql', 'db')
limit = config.getint('mysql', 'limit')

conn = MySQLdb.connect(host, user, password, db)


class Application(object):
    def __init__(self):
        print('Application.__init__')
        self.collections = CollectionAPI()

    @cherrypy.expose
    def index(self):
        cherrypy.response.header_list = [('Content-Type', 'text/html')]
        return '<body><h1>Data Collections Service</h1></body>.'

    @cherrypy.expose
    def version(self):
        """Return the version of this implementation.

        :returns: System capabilities in JSON format
        :rtype: string
        """
        cherrypy.response.header_list = [('Content-Type', 'text/plain')]
        return version

    @cherrypy.expose
    def features(self):
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


class NotImplemented(object):
    def index(self, **args):
        return 'Method not implemented.'


@cherrypy.popargs('collid')
class CollectionAPI(object):
    def __init__(self):
        self.members = MemberAPI()
        self.ops = NotImplemented()

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
            coll = Collection(conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        auxCap = capabilitiesFixed.copy()
        auxCap['restrictedtotype'] = coll.restrictedtotype

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps(auxCap)

    @cherrypy.expose
    def index(self, collid=None):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        if cherrypy.request.method == 'GET':
            return self.get(collid)

        if cherrypy.request.method == 'POST':
            self.post(collid)

        if cherrypy.request.method == 'PUT':
            self.put(collid)

        if cherrypy.request.method == 'DELETE':
            self.delete(collid)

        messDict = {'code': 0,
                    'message': 'Method %s not recognized/implemented!' % cherrypy.request.method}
        message = json.dumps(messDict)
        raise cherrypy.HTTPError(400, message)

    def delete(self, collid):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(400, message)

        try:
            coll = Collection(conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        coll.delete(conn)

        return ""

    def put(self, collid):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(400, message)

        jsonColl = json.loads(cherrypy.request.body.fp.read())

        try:
            coll = Collection(conn, collID=collID)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        # Read only the fields that we support
        try:
            owner = jsonColl['properties']['ownership'].strip()
        except:
            owner = None

        try:
            pid = jsonColl['pid'].strip()
        except:
            pid = None

        try:
            name = jsonColl['name'].strip()
        except:
            name = None

        try:
            jc = jsonColl['capabilities']
            restrictedtotype = jc['restrictedToType'].strip()
        except:
            restrictedtotype = None

        try:
            rule = jsonColl['rule'].strip()
        except:
            rule = None

        # FIXME I must check if the object coll is being updated as in the DB!
        coll.update(name=name, owner=owner, pid=pid,
                    restrictedtotype=restrictedtotype, rule=rule)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.toJSON()

    def post(self, collid):
        if collid is not None:
            messDict = {'code': 0,
                        'message': 'A collection ID (%s) was received while trying to create a Collection' % collID}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(400, message)

        jsonColl = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        try:
            owner = jsonColl['properties']['ownership'].strip()
        except:
            owner = None
        try:
            pid = jsonColl['pid'].strip()
        except:
            pid = None
        try:
            name = jsonColl['name'].strip()
        except:
            name = None
        try:
            jc = jsonColl['capabilities']
            restrictedtotype = jc['restrictedToType'].strip()
        except:
            restrictedtotype = None
        try:
            rule = jsonColl['rule'].strip()
        except:
            rule = None

        try:
            coll = Collection(conn, pid=pid, name=name)

            # Send Error 400
            msg = 'Collection with this PID and name already exists! (%s, %s)'
            messDict = {'code': 0,
                        'message': msg % (pid, name)}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)
        except:
            pass

        try:
            # It is important to call insert inline with an empty Collection!
            coll = Collection(None).insert(conn, owner=owner, pid=pid,
                                           name=name,
                                           restrictedtotype=restrictedtotype,
                                           rule=rule)
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

    def get(self, collid):
        if collid is None:
            # If no ID is given iterate through all collections in cursor
            coll = Collections(conn)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return JSONFactory(coll, Collection)

        try:
            coll = Collection(conn, collid=collid)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.toJSON().encode()


@cherrypy.popargs('memberid')
class MemberAPI(object):
    def __init__(self):
        """Constructor of the DataColl object."""
        pass

    @cherrypy.expose
    def properties(self, collid, memberid):
        return 'Not implemented!'

    @cherrypy.expose
    def index(self, collid, memberid=None):
        if cherrypy.request.method == 'GET':
            self.get(collid, memberid)

        if cherrypy.request.method == 'POST':
            self.post(collid, memberid)

        if cherrypy.request.method == 'PUT':
            self.put(collid, memberid)

        if cherrypy.request.method == 'DELETE':
            self.delete(collid, memberid)

        messDict = {'code': 0,
                    'message': 'Method %s not recognized/implemented!' % cherrypy.request.method}
        message = json.dumps(messDict)
        raise cherrypy.HTTPError(400, message)

    def get(self, collid, memberid):
        if memberid is None:
            membList = Members(conn, collid=collid)

            # If no ID is given iterate through all collections in cursor
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return JSONFactory(membList, Member)

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except:
            messDict = {'code': 0,
                        'message': 'Member %s or Collection %s not found'
                        % (memberID, collID)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return member.toJSON()

    def post(self, collid, memberid):
        if memberid is not None:
            messDict = {'code': 0,
                        'message': 'Member ID received while trying to create it!'}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        pid = jsonMemb.get('pid', None)
        location = jsonMemb.get('location', None)
        datatype = jsonMemb.get('datatype', None)
        checksum = jsonMemb.get('checksum', None)
        index = jsonMemb.get('mappings', {}).get('index', None)

        try:
            coll = Collection(conn, collID=collID)
        except:
            # Send Error 404
            messDict = {'code': 0,
                        'message': 'Collection %s not found!' % collID}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # Check if the collection accepts only a particular datatype
        if ((coll.restrictedtotype is not None) and
                (datatype != coll.restrictedtotype)):
            msg = 'Datatype error! Collection only accepts %s'
            messDict = {'code': 0,
                        'message': msg % coll.restrictedtotype}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        # FIXME Here we need to set also the datatype after checking the
        # restrictedToType attribute in the collection
        try:
            member = Member(None).insert(conn, collID=collID, pid=pid,
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

    def put(self, collid, memberid):
        if((collid is None) or (memberid is None)):
            messDict = {'code': 0,
                        'message': 'No member or collection ID was received!'}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(400, message)

        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        # Read only the fields that we support
        pid = jsonMemb.get('pid', None)
        location = jsonMemb.get('location', None)
        checksum = jsonMemb.get('checksum', None)
        datatype = jsonMemb.get('datatype', None)
        index = jsonMemb.get('mappings', {}).get('index', None)

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except:
            # FIXME We need to check here the datatype by querying the
            # collection and comparing with the restrictedToType attribute
            # Send Error 404
            msg = 'Member %s from Collection %s not found!'
            messDict = {'code': 0,
                        'message': msg % (memberid, collid)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        # Check that the index does not collide with existent IDs
        if index != memberid:
            try:
                Member(conn, collid=collid, memberid=index)
                # Send Error 400
                msg = 'Index %s is already used for Collection %s !'
                messDict = {'code': 0,
                            'message': msg % (index, collid)}
                message = json.dumps(messDict)
                cherrypy.response.headers['Content-Type'] = 'application/json'
                raise cherrypy.HTTPError(400, message)
            except:
                pass

        # Read the collection to check whether datatype is restricted
        try:
            coll = Collection(conn, collid=collid)
        except:
            msg = 'Error checking the collection properties!'
            messDict = {'code': 0,
                        'message': msg}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        # Check if the collection accepts only a particular datatype
        if ((coll.restrictedtotype is not None) and
                (datatype != coll.restrictedtotype)):
            msg = 'Datatype error! Collection only accepts %s'
            messDict = {'code': 0,
                        'message': msg % coll.restrictedtotype}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        member.update(conn, pid=pid, location=location, checksum=checksum,
                      datatype=datatype, memberid=index)

        # Read the member
        try:
            memb = Member(conn, collid=collid, memberid=index)
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

    def delete(self, collid, memberid):
        if((collid is None) or (memberid is None)):
            messDict = {'code': 0,
                        'message': 'No member or collection ID was received!'}
            message = json.dumps(messDict)
            raise cherrypy.HTTPError(400, message)

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except:
            msg = 'Member ID %s within collection ID %s not found'
            messDict = {'code': 0,
                        'message': msg % (memberid, collid)}
            message = json.dumps(messDict)

            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        member.delete(conn)

        return ""


if __name__ == '__main__':
    cherrypy.quickstart(Application(), script_name='/rda/datacoll')
