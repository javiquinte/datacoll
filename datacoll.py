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


class MembersAPI(object):
    @cherrypy.expose
    def GET(self, collID):
        return "members.GET(%d)" % int(collID)


class CollectionsAPI(object):
    # FIXME ! It is still not clear why here the handler looks for "index" and
    # not "GET".
    @cherrypy.expose(['index'])
    def GET(self):
        return "collections.GET"


class CollectionAPI(object):
    @cherrypy.expose
    def GET(self, collID):
        return "collection.GET(%d)" % int(collID)


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
                    if vpath.pop(1) != "members":
                        raise cherrypy.HTTPError(400, 'Bad Request')

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
