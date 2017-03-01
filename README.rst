Data Collection API implementation for GEOFON
---------------------------------------------

To do list
==========

* All the errors 401 related to unauthorized access are still missing.

License
=======
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Installation
============

The installation instructions are included in the package, but need first to be
generated. Follow the instructions in the next section to do it.

RDA API vs this implementation
==============================

In the following table we summarize the requests specified in the RDA Data
Collection API and the status of this implementation.

* In almost all cases if a wrong parameter is passed, the HTTP error should be
400 and not 404.

================================================= ======== ============= =================
  Request                                          Method   Implemented   What's missing?
------------------------------------------------- -------- ------------- -----------------
/features                                          GET        Yes
/collections                                       GET        Yes        
/collections                                       POST       Yes
/collections/{id}                                  DELETE     Yes
/collections/{id}                                  GET        Yes
/collections/{id}                                  PUT        Yes           Test
/collections/{id}/capabilities                     GET        Yes
/collections/{id}/ops/...                          ANY        No
/collections/{id}/members                          GET        Yes
/collections/{id}/members                          POST       Yes
/collections/{id}/members/{id}                     DELETE     Yes
/collections/{id}/members/{id}                     GET        Yes
/collections/{id}/members/{id}                     PUT        Migrate
/collections/{id}/members/{id}/properties/{prop}   DELETE     No
/collections/{id}/members/{id}/properties/{prop}   GET        Migrate
/collections/{id}/members/{id}/properties/{prop}   PUT        No
/collections/{id}/members/{id}/download            GET        Yes
================================================= ======== ============= =================


Documentation
=============

To get the documentation of the current version of the Data Collection Service
you please follow these steps:

1. Go to the "doc" subdirectory located where the package was decompressed.
Let's suppose it is "/var/www/rda/datacoll". ::

  $ cd /var/www/rda/datacoll/doc

2. Build the documentation. ::

  $ make latexpdf

3. Open the generated PDF file with an appropriate application (e.g. acroread,
evince, etc). The file will be located under the .build/latex directory. ::

  $ acroread _build/latex/DataCollectionsWS.pdf

Copy this file to the location most suitable to you for future use.
