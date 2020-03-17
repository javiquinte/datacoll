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
import gnupg
import datetime
import urllib
from pymongo import MongoClient
from dcmongo import Collection
from dcmongo import Collections
from dcmongo import Member
from dcmongo import Members
from dcmongo import JSONFactory

version = '0.3a1'
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

# conn = MySQLdb.connect(host, user, password, db)
client = MongoClient(host, 27017)
conn = client['datacoll']

# Create the object to verify the signature in tokens
try:
    gpg = gnupg.GPG(homedir='.gnupg')
except TypeError:
    gpg = gnupg.GPG(gnupghome='.gnupg')


def verifysignature(access_token):
    try:
        verified = gpg.decrypt(access_token)

    except Exception as e:
        msg = "invalid token"
        raise Exception("%s: %s" % (msg, str(e)))

    if verified.trust_level is None or verified.trust_level < verified.TRUST_FULLY:
        msg = "token has an invalid signature"
        raise Exception(msg)

    try:
        attributes = json.loads(verified.data)
        d1 = datetime.datetime.strptime(attributes['valid_until'], "%Y-%m-%dT%H:%M:%S.%fZ")
        lifetime = (datetime.datetime.utcnow() - d1).seconds

    except Exception as e:
        msg = "token has invalid validity"
        raise Exception("%s: %s" % (msg, str(e)))

    if lifetime <= 0:
        msg = "token is expired"
        raise Exception(msg)


def checktokensoft(f):
    def checktokenintern(*a, **kw):
        if kw.get('access_token', True) is None:
            del kw['access_token']

        if 'Authorization' in cherrypy.request.headers:
            assert cherrypy.request.headers['Authorization'].split()[0] == 'Bearer'
            kw['access_token'] = cherrypy.request.headers['Authorization'].split()[1]

        try:
            if kw.get('access_token', None) is not None:
                kw['access_token'] = urllib.parse.unquote(kw['access_token'])
                verifysignature(kw['access_token'])

        except Exception as e:
            messDict = {'code': 0,
                        'message': str(e)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        return f(*a, **kw)

    return checktokenintern


def checktokenhard(f):
    def checktokenintern(*a, **kw):
        if 'Authorization' in cherrypy.request.headers:
            assert cherrypy.request.headers['Authorization'].split()[0] == 'Bearer'
            kw['access_token'] = cherrypy.request.headers['Authorization'].split()[1]

        if kw.get('access_token', True) is None:
            del kw['access_token']

        try:
            kw['access_token'] = urllib.parse.unquote(kw['access_token'])
            verifysignature(kw['access_token'])

            return f(*a, **kw)

        except Exception as e:
            messDict = {'code': 0,
                        'message': str(e)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

    return checktokenintern


class Application(object):
    def __init__(self):
        self.collections = CollectionAPI()

    @cherrypy.expose
    def index(self):
        cherrypy.response.header_list = [('Content-Type', 'text/html')]
        return '<body><h1>Data Collections Service</h1></body>.'

    @cherrypy.expose
    def version(self):
        """Return the version of this implementation.

        :returns: System version in JSON format
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


@cherrypy.popargs('collid')
class CollectionAPI(object):
    def __init__(self):
        self.members = MemberAPI()

    @cherrypy.expose
    def capabilities(self, collid):
        """Return the capabilities of a collection.

        :param collid: Collection ID.
        :type collid: int
        :returns: The capabilities of the collection in JSON format.
        :rtype: string
        :raises: cherrypy.HTTPError
        """
        # For the time being, these are fixed collections.
        # To be modified in the future with mutable collections
        try:
            coll = Collection(conn, collid=collid)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        auxCap = capabilitiesFixed.copy()
        # TODO See if capabilities should stay out side from Collection
        # auxCap['restrictedtotype'] = coll.restrictedtotype

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps(auxCap).encode()

    @cherrypy.expose
    def index(self, collid=None, **kwargs):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        if cherrypy.request.method == 'GET':
            return self.get(collid, **kwargs)

        if cherrypy.request.method == 'POST':
            return self.post(collid, **kwargs)

        if cherrypy.request.method == 'PUT':
            return self.put(collid, **kwargs)

        if cherrypy.request.method == 'DELETE':
            return self.delete(collid, **kwargs)

        messDict = {'code': 0,
                    'message': 'Method %s not recognized/implemented!' % cherrypy.request.method}
        message = json.dumps(messDict)
        raise cherrypy.HTTPError(400, message)

    # @checktokenhard
    def delete(self, collid, **kwargs):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        try:
            coll = Collection(conn, collid=collid)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        coll.delete(conn)

        return ""

    # @checktokenhard
    def put(self, collid, **kwargs):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        jsonColl = json.loads(cherrypy.request.body.fp.read())

        try:
            coll = Collection(conn, collid=collid)
        except:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # FIXME I must check if the object coll is being updated as in the DB!
        coll.update(jsonColl)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.document

    # @checktokenhard
    def post(self, collid, **kwargs):
        # if collid is not None:
        #     messDict = {'code': 0,
        #                 'message': 'A collection ID (%s) was received while trying to create a Collection' % collid}
        #     message = json.dumps(messDict)
        #     cherrypy.response.headers['Content-Type'] = 'application/json'
        #     raise cherrypy.HTTPError(400, message)

        jsonColl = json.loads(cherrypy.request.body.fp.read())

        try:
            # FIXME This is not raising an Exception (expected) if the Collection do not exist
            coll = Collection(conn, collid)
            raise Exception

            # Send Error 400
            msg = 'Collection with this PID and name already exists! (%s, %s)'
            messDict = {'code': 0,
                        'message': msg % (collid, name)}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)
        except Exception:
            pass

        try:
            # It is important to call insert inline with an empty Collection!
            coll = Collection(conn, collid).insert(jsonColl)
        except Exception:
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection could not be inserted'}
            message = json.dumps(messDict)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        cherrypy.response.status = '201 Collection %s created' % str(collid)
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.document

    # @checktokensoft
    def get(self, collid=None, **kwargs):
        if collid is None:
            # If no ID is given iterate through all collections in cursor
            coll = Collections(conn)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return JSONFactory(coll, Collection)

        try:
            coll = Collection(conn, collid=collid)
        except Exception:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.document


class DownloadMemberAPI(object):
    @cherrypy.expose
    def index(self, collid, memberid, **kwargs):
        url = Member(collid, memberid).download()
        raise cherrypy.HTTPRedirect(url, 301)


@cherrypy.popargs('memberid')
class MemberAPI(object):
    def __init__(self):
        """Constructor of the DataColl object."""
        self.download = DownloadMemberAPI()

    @cherrypy.expose
    def properties(self, collid, memberid):
        return 'Not implemented!'

    @cherrypy.expose
    def index(self, collid, memberid=None, **kwargs):
        if cherrypy.request.method == 'GET':
            return self.get(collid, memberid, **kwargs)

        if cherrypy.request.method == 'POST':
            return self.post(collid, memberid, **kwargs)

        if cherrypy.request.method == 'PUT':
            return self.put(collid, memberid, **kwargs)

        if cherrypy.request.method == 'DELETE':
            return self.delete(collid, memberid, **kwargs)

        messDict = {'code': 0,
                    'message': 'Method %s not recognized/implemented!' % cherrypy.request.method}
        message = json.dumps(messDict)
        cherrypy.response.headers['Content-Type'] = 'application/json'
        raise cherrypy.HTTPError(400, message)

    # @checktokensoft
    def get(self, collid, memberid, **kwargs):
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
                        % (memberid, collid)}
            message = json.dumps(messDict)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return member.toJSON().encode()

    # @checktokenhard
    def post(self, collid, memberid, **kwargs):
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
            coll = Collection(conn, collid=collid)
        except:
            # Send Error 404
            messDict = {'code': 0,
                        'message': 'Collection %s not found!' % collid}
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
            member = Member(None).insert(conn, collid=collid, pid=pid,
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
        return member.toJSON().encode()

    # @checktokenhard
    def put(self, collid, memberid, **kwargs):
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
        return memb.toJSON().encode()

    # @checktokenhard
    def delete(self, collid, memberid, **kwargs):
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
    config = {'/': {'tools.trailing_slash.on': False}}
    cherrypy.server.socket_host = "0.0.0.0"
    cherrypy.quickstart(Application(), script_name='/rda/datacoll', config=config)
