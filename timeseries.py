#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Originally adapted from timeseries.py as part of the cm_api module
#
#     https://github.com/cloudera/cm_api/blob/cm-4.6/python/examples/timeseries.py

"""
Provide the aggregated timeseries for a given query in CSV format

Usage: %s [options] <query>

Options:
-h --help                          Show help
--host=<cm-server-host>            Specify a Cloudera Manager Server host
                                   Defaults to 'localhost'
--port=<cm-server-port>            Override the default Cloudera Manager Server port
                                   Defaults to '7180'
--version=<cm-server-api-version>  Define the Cloudera Manager Server API version
                                   Defaults to latest as defined in the cm_api python module
--user=<cm-server-user>            The Cloudera Manager user
                                   Defaults to 'admin'
--user=<cm-server-user-password>   The Cloudera Manager user password
                                   Defaults to 'admin'
--from_time=<from_time>            From time for the query, in the "YYYY-mm-ddTHH:MM" format
                                   Defaults to 30 minutes before 'to_time'
--to_time=<to_time>                To time for the query, in the "YYYY-mm-ddTHH:MM" format
                                   Defaults to current time if not specified
                                   
"""

from cm_api import api_client
from cm_api.api_client import ApiResource
from datetime import datetime, timedelta
import getopt
import inspect
import logging
import sys
import textwrap

LOG = logging.getLogger(__name__)

CM_API_MAXIMUM_TIME_WINDOW_DAY = 28
DEFAULT_FROM_TIME_WINDOW_MIN = 30

class TimeSeriesQuery(object):
    """
    """

    def query(self, host, port, version, user, password, from_time, to_time, query):
        return ApiResource(host, port, user, password, False, version).query_timeseries(query, from_time.isoformat(), to_time.isoformat())

def do_print(query, query_responses):
    print "TIMESERIES," + query.upper()
    for entity in query_responses:
        for metric in query_responses[entity]:
            if query_responses[entity][metric]:
                print ""
                print "ENTITY,TIMESTAMP," + metric.upper()
            for datas in query_responses[entity][metric]:
                for data in datas:
                    print entity + "," + str(data.timestamp) + "," + str(data.value)

def do_query(host, port, version, user, password, from_time, to_time, query):
    tsquery = TimeSeriesQuery()
    time_tranches = []
    while (to_time - from_time).days > CM_API_MAXIMUM_TIME_WINDOW_DAY:
        time_tranches.append([from_time, from_time + timedelta(days=CM_API_MAXIMUM_TIME_WINDOW_DAY)])
        from_time = (from_time + timedelta(days=CM_API_MAXIMUM_TIME_WINDOW_DAY))
    time_tranches.append([from_time, to_time])
    query_responses = {}
    for time_tranche in time_tranches:
        for response in tsquery.query(host, port, version, user, password, time_tranche[0], time_tranche[1], query):
            if response.warnings:
                print >> sys.stderr, "Warnings: %s" % (response.warnings)
            if response.errors:
                print >> sys.stderr, "Errors: %s" % (response.errors)
            if response.timeSeries:
                for timeseries in response.timeSeries:
                    metadata = timeseries.metadata
                    if timeseries.data:
                        query_responses[metadata.entityName] = query_responses.get(metadata.entityName, {})
                        query_responses[metadata.entityName][metadata.metricName] = query_responses[metadata.entityName].get(metadata.metricName, [])
                        query_responses[metadata.entityName].get(metadata.metricName).append(timeseries.data)
    do_print(query, query_responses)    

def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))

def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)

def main(argv):
    setup_logging(logging.INFO)

    host = 'localhost'
    port = 7180
    version = api_client.API_CURRENT_VERSION
    user = 'admin'
    password = 'admin'    
    from_time = None
    to_time = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "host=", "port=", "version=", "user=", "password=", "from_time=", "to_time="])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1

    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            return -1
        elif option in ("--host"):
            host = value;
        elif option in ("--port"):
            port = value;
        elif option in ("--version"):
            version = value;
        elif option in ("--user"):
            user = value;
        elif option in ("--password"):
            password = value;
        elif option in ("--from_time"):
            try:
                from_time = datetime.strptime(value, "%Y-%m-%dT%H:%M")
            except:
                print >> sys.stderr, "Unable to parse the from time: " + value
                return -1
        elif option in ("--to_time"):
            try:
                to_time = datetime.strptime(value, "%Y-%m-%dT%H:%M")
            except:
                print >> sys.stderr, "Unable to parse the to time: " + value
                return -1
        else:
            print >> sys.stderr, "Unknown flag: ", option
            return -1

    if args:
        if to_time is None:
            to_time = datetime.now()
        if from_time is None:
            from_time = (to_time - timedelta(minutes=DEFAULT_FROM_TIME_WINDOW_MIN))
        do_query(host, port, version, user, password, from_time, to_time, args[0])
        return 0
    else:
        usage()
        return -1

if __name__ == '__main__':
    sys.exit(main(sys.argv))
