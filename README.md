ApacheLogPlayback
=================

Overview
---------

Send HTTP requests at actual intervals of apache accesslog.

Example
---------------

```
$ ./access_log_parser.py --convert_unixtime -i $(accesslog) | sort -k 1,1 -n | ./access_log_playback.py --host www.sample.com > result.tsv
```

Requirements
---------------

The scripts are checked under Python 3.3.0, and uses the below libraries.

* docopt>=0.6.1
* python-dateutil>=2.2
* requests>=2.4.1
* schema>=0.3.1

Limitations
---------------

* `access_log_playback.py` needs sorted accesslog by received time in ascending order.
* Supports GET method only.
    - The other methods may be supported if many demands of implementation are.

License
---------------

Please see LICENSE.
