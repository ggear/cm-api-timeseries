# CM API Time-series

Provide the aggregated timeseries for a given query in CSV format

# Requirements

* Python 2.7+
* CM API Python Client

# Install

The CM API Python Client is a dependency and must be installed via:

```bash
git clone git://github.com/cloudera/cm_api.git
cd cm_api/python
python setup.py install
```

# Running

Example usages:

```bash
./timeseries.py
./timeseries.py --help
./timeseries.py "select cpu_percent"
./timeseries.py "select total_cpu_user where roleType=DATANODE"
./timeseries.py "select (waiting_maps + waiting_reduces) where roleType=JOBTRACKER"
./timeseries.py --from_time 2013-08-29T10:00 --to_time 2013-08-29T16:00 "select (waiting_maps + waiting_reduces) where roleType=JOBTRACKER"
./timeseries.py --host my.cm.server.com --port 80 --version 4 --user my-user --password my-user-password "select (waiting_maps + waiting_reduces) where roleType=JOBTRACKER"
```

For details on how to define valid queries, see the [CM tsquery language reference](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM4Ent/latest/Cloudera-Manager-Diagnostics-Guide/cmdg_tsquery.html).