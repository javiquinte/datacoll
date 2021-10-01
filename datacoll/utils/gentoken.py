#!/usr/bin/env python3

"""Client for a Data Collection Service

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

   :Copyright:
       2016-2017 Javier Quinteros, GEOFON, GFZ Potsdam <geofon@gfz-potsdam.de>
   :License:
       GPLv3
   :Platform:
       Linux

.. moduleauthor:: Javier Quinteros <javier@gfz-potsdam.de>, GEOFON, GFZ Potsdam
"""

import os
import argparse
import datetime
import jwt
from datacoll import __version__


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', default=os.path.expanduser('~/.eidajwt'),
                        help='Output file where to store the JWT token')
    parser.add_argument('-d', '--dir', default=os.path.expanduser('~'),
                        help='Directory where the ".ssh" directory with keys can be found')
    parser.add_argument('-m', '--mail', help='E-Mail address to encode in the token')
    parser.add_argument('-n', '--cn', help='Full name to encode in the token')
    parser.add_argument('-e', '--exp', choices=['1d', '2d', '1w', '2w', '1m'], default='1d', help='Expiration time')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + __version__,
                        help='Show version information.')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Controls the verbosity of this script')
    args = parser.parse_args()

    # Check that there is an expiration for the token
    if args.exp == '1d':
        delta = datetime.timedelta(days=1)
    elif args.exp == '2d':
        delta = datetime.timedelta(days=2)
    elif args.exp == '1w':
        delta = datetime.timedelta(days=7)
    elif args.exp == '2w':
        delta = datetime.timedelta(days=14)
    elif args.exp == '1m':
        delta = datetime.timedelta(days=30)
    else:
        raise Exception('Invalid expiration for the token!')

    payload = {'memberof': '/epos/datacoll;/;/epos',
               'mail': args.mail,
               'cn': args.cn,
               'exp': datetime.datetime.utcnow() + delta,
               'iat': datetime.datetime.utcnow(),
               'iss': 'EAS'}

    # exp: Registered Claim Name declaring expiration
    # iat: Registered Claim Name declaring when this token was issued
    # iss: Registered Claim Name declaring the entity issuing this token

    with open(os.path.join(args.dir, '.ssh', 'id_rsa')) as fin:
        private_key = fin.read()
    # Dump the dictionary in a string and sign it with PGP
    digSign = jwt.encode(payload, private_key, algorithm='RS256')

    # Write the JWT in the output file
    with open(args.output, 'w') as fout:
        fout.write(digSign)


if __name__ == '__main__':
    main()
