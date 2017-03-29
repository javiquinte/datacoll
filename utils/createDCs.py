#!/usr/bin/env python

import json
import irodsInterface
from irods.models import DataObject
from irods.models import Collection
import urllib2 as ul

years = ['1993']
nets = ['GE']
stations = ['DSB', 'MORC', 'PMG']
channels = ['BHN.D','BHE.D',  'BHZ.D', 'HHN.D', 'HHE.D', 'HHZ.D']

dcUrl = 'http://localhost:8080/rda/datacoll'

root = '/geofonBak/archive'
i = irodsInterface.irodsInterface('irods.cfg')
i.connect()

def createColl(name):
    """Create a collection with the given name.

    Return either the collection or an error message (both in json format).
    """
    print 'Creating collection with name = %s' % name

    jsonColl = {
        "pid": name,
        "name": name,
        "capabilities": {
            "restrictedToType": "miniSEED"
        },
        "properties": {
            "ownership": "geofon@gfz-potsdam.de"
        }
    }

    req = ul.Request('%s/collections' % dcUrl, data=json.dumps(jsonColl))
    req.add_header("Content-Type",'application/json')
    # Create a collection
    try:
        u = ul.urlopen(req)
        return json.loads(u.read())
    except Exception as e:
        return {'message': 'Error creating collection'}


def createMember(collID, do):
    """Create a member in a collection based on the given DataObject.

    Return either the member or an error message (both in json format).
    """
    print 'Creating member with name = %s' % do.name

    jsonMember = {"location": "http://localhost:8000/api/registered" + \
                  do.path + "?download=true",
                  "checksum": do.checksum,
                  "datatype": "miniSEED"
                 }

    req = ul.Request('%s/collections/%d/members' %
                     (dcUrl, collID), data=json.dumps(jsonMember))
    req.add_header("Content-Type",'application/json')
    # Create a member
    try:
        u = ul.urlopen(req)
        return json.loads(u.read())
    except Exception as e:
        return {'message': 'Error creating member'}


for yc in i.listDir(root).subcollections:
    if yc.name not in years:
        continue

    for nc in yc.subcollections:
        if nc.name not in nets:
            continue

        for sc in nc.subcollections:
            if sc.name not in stations:
                continue

            for cc in sc.subcollections:
                if cc.name not in channels:
                    continue
                jsonColl = createColl('%s.%s.%s.%s' % (nc.name, sc.name,
                                                       cc.name[:3], yc.name))

                if 'message' in jsonColl.keys():
                    print str(jsonColl)

                for f in cc.data_objects:
                    if f.name.startswith(nc.name + '.' + sc.name + '.'):
                        try:
                            int(f.name[-3])
                            createMember(jsonColl['id'], f)
                        except:
                            continue
