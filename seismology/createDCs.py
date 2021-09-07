#!/usr/bin/env python3

import json
import os
import argparse
import hashlib
import urllib.request as ul

version = '0.1'

years = ['1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000',
         '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008',
         '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016']
nets = ['GE']
# stations = ['GIO', 'DSB', 'KBS', 'MLR', 'MORC', 'PMG', 'STU', 'WLF']
channels = ['BHN.D', 'BHE.D', 'BHZ.D', 'HHN.D', 'HHE.D', 'HHZ.D']

dcUrl = 'http://localhost:8080/rda/datacoll'
root = '/geofonZone/archive'


class DigitalObject():
    """Representation of a digital object"""
    def __init__(self, uri: str, checksum: str = None):
        # Possibly is a path
        if os.path.isfile(uri) and (checksum is None):
            # Calculate checksum
            checksum = hashlib.md5(uri).hexdigest()

        self.uri = uri
        self.checksum = checksum


def createColl(name):
    """Create a collection with the given name.

    Return either the collection or an error message (both in json format).
    """
    print('Creating collection with name = %s' % name)

    jsoncoll = {
        "name": name,
        "capabilities": {
            "restrictedToType": "application/vnd.fdsn.mseed"
        },
        "properties": {
            "ownership": "geofon@gfz-potsdam.de"
        }
    }

    req = ul.Request('%s/collections' % dcUrl, data=json.dumps(jsoncoll).encode())
    req.add_header("Content-Type", 'application/json')
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
    # print 'Creating member with name = %s' % do.name

    jsonmember = {"location": "http://sec24c79.gfz-potsdam.de:8000/api/registered" + do.uri,  # + "?download=true",
                  "checksum": do.checksum,
                  "datatype": "application/vnd.fdsn.mseed"
                 }

    req = ul.Request('%s/collections/%d/members' %
                     (dcUrl, collID), data=json.dumps(jsonmember).encode())
    req.add_header("Content-Type", 'application/json')
    # Create a member
    try:
        u = ul.urlopen(req)
        return json.loads(u.read())
    except Exception as e:
        return {'message': 'Error creating member'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', default=os.path.abspath(''),
                        help='Root directory of the SDS structure')
    parser.add_argument('-a', '--authentication', default=os.path.expanduser('~/.eidatoken'),
                        help='File containing the token to use during the authentication process')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version,
                        help='Show version information.')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Controls the verbosity of this script')
    args = parser.parse_args()

    for yc in os.listdir(args.data):
        if yc not in years:
            continue

        for nc in os.listdir(os.path.join(args.data, yc)):
            if nc not in nets:
                continue

            for sc in os.listdir(os.path.join(args.data, yc, nc)):
                # if sc.name not in stations:
                #     continue

                for cc in os.listdir(os.path.join(args.data, yc, nc, sc)):
                    if cc not in channels:
                        continue
                    jsoncoll = createColl('%s.%s.%s.%s' % (nc, sc, cc[:3], yc))

                    if 'message' in jsoncoll.keys():
                        print(str(jsoncoll))

                    for f in os.listdir(os.path.join(args.data, yc, nc, sc, cc)):
                        if f.startswith(nc + '.' + sc + '.'):
                            try:
                                # Check that the last 3 chars are the day of the year (int)
                                int(f[-3])
                                fullpath = os.path.join(args.data, yc, nc, sc, cc, f)
                                do = DigitalObject(fullpath)
                                createMember(jsoncoll['id'], do)
                            except Exception:
                                continue


if __name__ == '__main__':
    main()

