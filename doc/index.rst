.. Data Collections WS documentation master file, created by
   sphinx-quickstart on Mon Nov 14 15:17:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Data Collections WS's documentation!
===============================================

Contents:

.. toctree::
   :maxdepth: 2

Summary
=======

Building collections within diverse domains and then sharing or expanding them
across disciplines should enable common tools for end-users and e-infrastructure
providers. Individual disciplinary communities can directly benefit if such
tools are made widely available, and cross-community data sharing can benefit
from increased unification between collection models and implementations. PID
providers may benefit from marketing additional services on collections.

A living API prototype has been made available by the RDA working group at
http://rdacollectionswg.github.io/apidocs/# .

More information about the *Research Data Collections* working group can be
found at https://www.rd-alliance.org/groups/pid-collections-wg.html .

Installation
============

Requirements
------------

* python 3.6
* cherryPy for Python3
* mysql DB
* mysql driver

.. _download:

Download
--------

Change to a directory where you want to run the service and get the source
from the git repository at https://git.gfz-potsdam.de/javier/datacoll ::

    $ git clone https://git.gfz-potsdam.de/javier/datacoll
    $ cd datacoll

.. note ::
   Nightly builds will be available soon from Github.

This location will depend on the location of the root (in the file system)
for your web server.

.. _installation:

Installation
------------

This version of the Data Collection System runs as a standalone application
using the `cherrypy` web framework.
There is no need to deploy it on top of Apache.

But first the dependencies must be installed. ::

    $ python3.6 -m pip install cherrypy
    $ sudo apt install python3.6-dev
    $ python3.6 -m pip install mysqlclient

You can start the application by running: ::

    $ python3.6 datacoll.py
    ENGINE Listening for SIGHUP.
    ENGINE Listening for SIGTERM.
    ENGINE Listening for SIGUSR1.
    ENGINE Bus STARTING
    CherryPy Checker:
    The Application mounted at '/rda/datacoll' has an empty config.
    ENGINE Started monitor thread 'Autoreloader'.
    ENGINE Started monitor thread '_TimeoutMonitor'.
    ENGINE Serving on http://127.0.0.1:8080
    ENGINE Bus STARTED

The system will listen to the port 8080.

.. _configuration-options-extra:

Configuration options
^^^^^^^^^^^^^^^^^^^^^

The configuration file contains two sections up to this moment.

Service
"""""""

`verbosity` controls the amount of output send to the logging system depending
of the importance of the messages. The levels are: 1) Error, 2) Warning, 3)
Info and 4) Debug.

.. _service_configuration:

.. code-block:: ini

    [Service]
    # Possible values are:
    # CRITICAL, ERROR, WARNING, INFO, DEBUG
    verbosity = INFO

MySQL
"""""

In the mysql section a server must be defined, from which the actual data
collections can be retrieved and also stored.

.. code-block:: ini

    [mysql]
    host = localhost
    user = dcusername
    password = dcpassword
    db = datacoll
    limit = 500

Installation problems
^^^^^^^^^^^^^^^^^^^^^

Always check the log output for clues.

If you visit http://127.0.0.1:8080/rda/datacoll/version on your machine
you should see the version information of the deployed service ::

    0.2a1

If this information cannot be retrieved, the installation was not successful.
If this **does** show up, check that the information there looks correct.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
