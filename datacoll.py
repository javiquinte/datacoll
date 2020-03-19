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
# from bson.objectid import ObjectId
from bson.json_util import dumps
from dcmongo import Collection
from dcmongo import Collections
from dcmongo import Member
from dcmongo import Members
from dcmongo import JSONFactory
from dcmongo import DCEncoder

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
            message = json.dumps(messDict, cls=DCEncoder)
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
            message = json.dumps(messDict, cls=DCEncoder)
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
        return json.dumps(syscapab, cls=DCEncoder)


@cherrypy.popargs('collid')
class CollectionAPI(object):
    def __init__(self):
        self.members = MemberAPI()

    @cherrypy.expose
    def capabilities(self, collid):
        """Return the capabilities of a collection.

        :param collid: Collection ID.
        :type collid: str
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
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        auxCap = capabilitiesFixed.copy()
        # TODO See if capabilities should stay out side from Collection
        # auxCap['restrictedtotype'] = coll.restrictedtotype

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps(auxCap, cls=DCEncoder).encode('utf-8')

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
        message = json.dumps(messDict, cls=DCEncoder)
        raise cherrypy.HTTPError(400, message)

    # @checktokenhard
    def delete(self, collid, **kwargs):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        try:
            coll = Collection(conn, collid=collid)
        except Exception:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        coll.delete()

        return ""

    # @checktokenhard
    def put(self, collid, **kwargs):
        if collid is None:
            messDict = {'code': 0,
                        'message': 'No collection ID was received!'}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        jsonColl = json.loads(cherrypy.request.body.fp.read())

        try:
            coll = Collection(conn, collid=collid)
        except Exception:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        # TODO I must check if the object coll is being updated as in the DB!
        coll.update(jsonColl)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return coll.document

    # @checktokenhard
    def post(self, collid, **kwargs):
        jsonColl = json.loads(cherrypy.request.body.fp.read())

        # _id must always be a str
        if isinstance(collid, bytes):
            collid = collid.decode('utf-8')
        elif collid is not None:
            collid = str(collid)

        try:
            # This should raise an Exception if the Collection does not exist
            coll = Collection(conn, collid)

            # Otherwise send Error 400
            msg = 'Collection with this ID already exists! (%s)'
            messDict = {'code': 0,
                        'message': msg % (collid)}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)
        except Exception:
            pass

        try:
            # It is important to call insert inline with an empty Collection!
            insertedid = Collection(conn, collid).insert(jsonColl)
            if isinstance(insertedid, bytes):
                insertedid = insertedid.decode('utf-8')

            coll = Collection(conn, insertedid)
        except Exception:
            # Send Error 400
            messDict = {'code': 0,
                        'message': 'Collection could not be inserted'}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        cherrypy.response.status = '201 Collection %s created' % insertedid
        cherrypy.response.headers['Content-Type'] = 'application/json'

        result = json.dumps(coll.document, cls=DCEncoder)
        return result.encode('utf-8')

    # @checktokensoft
    def get(self, collid=None, **kwargs):
        if collid is None:
            # If no ID is given iterate through all collections in cursor
            coll = Collections(conn)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return JSONFactory(coll)

        try:
            coll = Collection(conn, collid=collid)
        except Exception:
            messDict = {'code': 0,
                        'message': 'Collection ID %s not found' % collid}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        result = json.dumps(coll.document, cls=DCEncoder)
        return result.encode('utf-8')


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
        cherrypy.response.headers['Content-Type'] = 'application/json'
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
        message = json.dumps(messDict, cls=DCEncoder)
        raise cherrypy.HTTPError(400, message)

    # @checktokensoft
    def get(self, collid, memberid, **kwargs):
        cherrypy.response.headers['Content-Type'] = 'application/json'

        if memberid is None:
            try:
                membList = Members(conn, collid=collid)
            except Exception:
                messDict = {'code': 0,
                            'message': 'Collection %s not found' % collid}
                message = json.dumps(messDict, cls=DCEncoder)
                raise cherrypy.HTTPError(404, message)

            # If no ID is given iterate through all collections in cursor
            return dumps(membList).encode('utf-8')

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except Exception:
            messDict = {'code': 0,
                        'message': 'Member %s or Collection %s not found'
                        % (memberid, collid)}
            message = json.dumps(messDict, cls=DCEncoder)
            raise cherrypy.HTTPError(404, message)

        result = json.dumps(member.document, cls=DCEncoder)
        return result.encode('utf-8')

    # @checktokenhard
    def post(self, collid, memberid, **kwargs):
        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        # _id must always be a str
        if isinstance(collid, bytes):
            collid = collid.decode('utf-8')
        elif collid is not None:
            collid = str(collid)
        # _id must always be a str
        if isinstance(memberid, bytes):
            memberid = memberid.decode('utf-8')
        elif memberid is not None:
            memberid = str(memberid)

        try:
            coll = Collection(conn, collid=collid)
        except:
            # Send Error 404
            messDict = {'code': 0,
                        'message': 'Collection %s not found!' % collid}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        try:
            # This should raise an Exception if the Member does not exist
            m = Member(conn, collid, memberid)

            # Otherwise send Error 400
            msg = 'Member with this ID already exists! (%s, %s)'
            messDict = {'code': 0,
                        'message': msg % (collid, memberid)}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)
        except Exception:
            pass

        # FIXME Here we need to set also the datatype after checking the
        # restrictedToType attribute in the collection
        try:
            insertedid = Member(conn, collid, memberid).insert(jsonMemb)
            if isinstance(insertedid, bytes):
                insertedid = insertedid.decode('utf-8')

            memb = Member(conn, collid, insertedid)
        except Exception:
            msg = 'Member could not be inserted'
            messDict = {'code': 0,
                        'message': msg}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.log(message, traceback=True)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, msg)

        cherrypy.response.status = '201 Member created (%s)' % insertedid
        cherrypy.response.headers['Content-Type'] = 'application/json'
        result = json.dumps(memb.document, cls=DCEncoder)
        return result.encode('utf-8')

    # @checktokenhard
    def put(self, collid, memberid, **kwargs):
        if((collid is None) or (memberid is None)):
            messDict = {'code': 0,
                        'message': 'No member or collection ID was received!'}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        jsonMemb = json.loads(cherrypy.request.body.fp.read())

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except Exception:
            msg = 'Member %s from Collection %s not found!'
            messDict = {'code': 0,
                        'message': msg % (memberid, collid)}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        # Check that the index does not collide with existent IDs
        # if index != memberid:
        #     try:
        #         Member(conn, collid=collid, memberid=index)
        #         # Send Error 400
        #         msg = 'Index %s is already used for Collection %s !'
        #         messDict = {'code': 0,
        #                     'message': msg % (index, collid)}
        #         message = json.dumps(messDict, cls=DCEncoder)
        #         cherrypy.response.headers['Content-Type'] = 'application/json'
        #         raise cherrypy.HTTPError(400, message)
        #     except:
        #         pass

        # Check if the collection accepts only a particular datatype
        # if ((coll.restrictedtotype is not None) and
        #         (datatype != coll.restrictedtotype)):
        #     msg = 'Datatype error! Collection only accepts %s'
        #     messDict = {'code': 0,
        #                 'message': msg % coll.restrictedtotype}
        #     message = json.dumps(messDict, cls=DCEncoder)
        #     cherrypy.response.headers['Content-Type'] = 'application/json'
        #     raise cherrypy.HTTPError(400, message)

        # TODO I must check if the object is being updated as in the DB!
        member.update(jsonMemb)

        # Read the member
        # try:
        #     memb = Member(conn, collid=collid, memberid=index)
        # except:
        #     # Send Error 400
        #     msg = 'Member seems not to be properly saved.'
        #     messDict = {'code': 0,
        #                 'message': msg}
        #     message = json.dumps(messDict, cls=DCEncoder)
        #     cherrypy.response.headers['Content-Type'] = 'application/json'
        #     raise cherrypy.HTTPError(400, message)

        cherrypy.response.headers['Content-Type'] = 'application/json'
        return member.document

    # @checktokenhard
    def delete(self, collid, memberid, **kwargs):
        if((collid is None) or (memberid is None)):
            messDict = {'code': 0,
                        'message': 'No member or collection ID was received!'}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(400, message)

        try:
            member = Member(conn, collid=collid, memberid=memberid)
        except Exception:
            msg = 'Member ID %s within collection ID %s not found'
            messDict = {'code': 0,
                        'message': msg % (memberid, collid)}
            message = json.dumps(messDict, cls=DCEncoder)
            cherrypy.response.headers['Content-Type'] = 'application/json'
            raise cherrypy.HTTPError(404, message)

        member.delete()

        return ""


if __name__ == '__main__':
    config = {'/': {'tools.trailing_slash.on': False}}
    cherrypy.server.socket_host = "0.0.0.0"
    cherrypy.quickstart(Application(), script_name='/rda/datacoll', config=config)
