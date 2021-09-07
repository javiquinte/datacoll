#!/usr/bin/env python3

import json
import os
import argparse
import urllib.request as ul
from datacoll.core import Collection
from datacoll.core import Member

version = '0.1'

dcUrl = 'http://localhost:8080/rda/datacoll'
root = '/geofonZone/archive'

# def createColl(name, owner: str = 'unknown'):
#     """Create a collection with the given name.
#
#     Return either the collection or an error message (both in json format).
#     """
#     print('Creating collection with name = %s' % name)
#
#     jsoncoll = {
#         "name": name,
#         "capabilities": {},
#         "properties": {
#             "ownership": owner
#         }
#     }
#
#     req = ul.Request('%s/collections' % dcUrl, data=json.dumps(jsoncoll).encode())
#     req.add_header("Content-Type", 'application/json')
#     # Create a collection
#     try:
#         u = ul.urlopen(req)
#         return json.loads(u.read())
#     except Exception as e:
#         return {'message': 'Error creating collection'}
#
#
# def createMember(collID, do):
#     """Create a member in a collection based on the given DataObject.
#
#     Return either the member or an error message (both in json format).
#     """
#     # print 'Creating member with name = %s' % do.name
#
#     jsonmember = {"location": do.uri,  # + "?download=true",
#                   "checksum": do.checksum,
#                   "datatype": do.mimetype
#                  }
#
#     req = ul.Request('%s/collections/%d/members' %
#                      (dcUrl, collID), data=json.dumps(jsonmember).encode())
#     req.add_header("Content-Type", 'application/json')
#     # Create a member
#     try:
#         u = ul.urlopen(req)
#         return json.loads(u.read())
#     except Exception as e:
#         return {'message': 'Error creating member'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', default=os.path.abspath(''),
                        help='Root directory where the members of the collection are stored')
    parser.add_argument('-a', '--authentication', default=os.path.expanduser('~/.eidatoken'),
                        help='File containing the token to use during the authentication process')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version,
                        help='Show version information.')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Controls the verbosity of this script')
    args = parser.parse_args()

    coll = Collection(directory=args.data)

    # Iterate through files in directory
    for file in os.listdir(args.data):
        fullpath = os.path.join(args.data, file)
        coll.addmember(Member(location=fullpath))

    print(coll.json)
    for member in coll:
        print(member.json)


if __name__ == '__main__':
    main()

