![CubeDb][image_logo]

[![Build Status](https://travis-ci.org/cubedb/cubedb.svg?branch=master)](https://travis-ci.org/cubedb/cubedb)

# CubeDB

A simple, stupid and fast in-memory multi-key counter store. See [demo](https://cubedb.github.io/homicide-report/).

## Synopsis

Created by the Badoo BI tech team CubeDB lets us explore tens of millions of incoming events
**live**. The database keeps all of it's data in memory and is designed to do one thing only: answer
simple queries about the number of data points in partition intervals really fast, i.e. within tens
of milliseconds.

CubeDB operates on Cubes. A Cube is a collection of data points split into partitions. Every data
point has a partitioning field, a set of string dimension fields and a set of integer metric fields. A
partition field is used for retrieving intervals of partitions containing the data points.

Here at Badoo a single Cube corresponds to a single event type, partitions represent ordered time
intervals (hours and days), and dimension fields are event parameters. All of the incoming events
are inserted immediately upon receival and become instantly explorable through the interface built
on top of CubeDB.

CubeDB is a server-side reimplementation of [Crossfilter](https://square.github.io/crossfilter/), so
we also have an [explanation in terms of the library](README-Crossfilter.md).

There's also an [explanation in terms of SQL](README-SQL.md).

## Typical usage scenario

Imagine a data set of 30 million events with a common set of parameters partitioned by 24 hours and loaded into
CubeDB.

CubeDB lets you answer the following example questions:
- count the number of events in hourly partitions
- count the number of events in hourly partitions with given parameter values
- count the number of events in a subinterval of hourly partitions
- group the events in hours by parameter values, and count events in the groups
- group the events by one value, filter by another, and count events in the groups

And so on. The results are presented in JSON meant to be used for data visualisation purposes, all
available in dozens of milliseconds.

## Usage example

For installation see the "Installing and compiling" section below. Also a Docker image is available
as as alternative way to test run CubeDB - for which see the "Running on Docker" section.

Given a working CubeDB instance on http://locahost:9998 let's see what's inside:

```shell
> curl http://localhost:9998/v1/stats
{
  "header": {
    "requestTs": 1508938484932,
    "processingTimeMs": 0,
    "request": {},
    "params": {}
  },
  "response": {
    "numLargeBlocks": 0,
    "columnSizeBytes": 0,
    "numPartitions": 0,
    "numReadOnlyPartitions": 0,
    "metricSizeBytes": 0,
    "numRecords": 0,
    "numCubes": 0,
    "cubeStats": {},
    "columnBlocks": 0,
    "metricBlocks": 0
  }
}
```

Notice that there are no cubes and no records in CubeDB yet.

Now insert a single data point:

```shell
> read -d '' json <<"EOF"
[
  {
    "partition": "partition-01",
    "counters": {
      "c": 1
    },
    "fields": {
      "field_1": "1"
    },
    "cubeName": "test_cube"
  }
]
EOF
> echo "$json" | curl -s --data-binary "@-" -H "Content-Type: text/json" -X POST http://localhost:9998/v1/insert`
{
  "header": {
    "requestTs": 1508939480327,
    "processingTimeMs": 24,
    "request": null,
    "params": null
  },
  "response": {
    "numInsertedRows": 1
}
```

Data points are inserted as a list of JSON maps, each representing a single data point. Here the
list contains a single data point only.

A cube was created with a single partition:

```shell
> curl http://localhost:9998/v1/stats
{
  "header": {
    "requestTs": 1508939607001,
    "processingTimeMs": 25,
    "request": {},
    "params": {}
  },
  "response": {
    "numLargeBlocks": 0,
    "columnSizeBytes": 2,
    "numPartitions": 1,
    "numReadOnlyPartitions": 0,
    "metricSizeBytes": 8,
    "numRecords": 1,
    "numCubes": 1,
    "cubeStats": {
      "test_cube": {
        "cubeFieldToValueNum": {
          "field_1": 2
        },
        "cubeMinPartition": "partition-01",
        "numLargeBlocks": 0,
        "columnSizeBytes": 2,
        "numPartitions": 1,
        "numReadOnlyPartitions": 0,
        "metricSizeBytes": 8,
        "numRecords": 1,
        "cubeMaxPartition": "partition-01",
        "columnBlocks": 1,
        "metricBlocks": 1
      }
    },
    "columnBlocks": 1,
    "metricBlocks": 1
  }
}
```

Put one more data point into another partition:

```shell
> read -d '' json <<"EOF"
[
  {
    "partition": "partition-02",
    "counters": {
      "c": 1
    },
    "fields": {
      "field_1": "1"
    },
    "cubeName": "test_cube"
  }
]
EOF
> echo $json | curl -s --data-binary "@-" -H "Content-Type: text/json" -X POST http://localhost:9998/v1/insert`
{
  "header": {
    "requestTs": 1508941112127,
    "processingTimeMs": 4,
    "request": null,
    "params": null
  },
  "response": {
    "numInsertedRows": 1
  }
}
```

Now let's see how many data points are there in the cube:

```shell
> curl http://localhost:9998/v1/stats
{
  "header": {
    "requestTs": 1508941384208,
    "processingTimeMs": 0,
    "request": {},
    "params": {}
  },
  "response": {
    "numLargeBlocks": 0,
    "columnSizeBytes": 4,
    "numPartitions": 2,
    "numReadOnlyPartitions": 0,
    "metricSizeBytes": 16,
    "numRecords": 2,
    "numCubes": 1,
    "cubeStats": {
      "test_cube": {
        "cubeFieldToValueNum": {
          "field_1": 2
        },
        "cubeMinPartition": "partition-01",
        "numLargeBlocks": 0,
        "columnSizeBytes": 4,
        "numPartitions": 2,
        "numReadOnlyPartitions": 0,
        "metricSizeBytes": 16,
        "numRecords": 2,
        "cubeMaxPartition": "partition-02",
        "columnBlocks": 2,
        "metricBlocks": 2
      }
    },
    "columnBlocks": 2,
    "metricBlocks": 2
  }
}
```

Notice that there are two lexicographically sorted partitions in the Cube. Also in the Cube there's
only one dimension field with two possible values (a null value and the string supplied in inserts).

Retrieve data from the Cube:

```shell
> curl http://localhost:9998/v1/test_cube/last/100
{
  "header": {
    "requestTs": 1508942013048,
    "processingTimeMs": 36,
    "request": {},
    "params": {
      "range": [
        "100"
      ],
      "cubeName": [
        "test_cube"
      ]
    }
  },
  "response": {
    "p": {
      "partition-01": {
        "c": 1
      },
      "partition-02": {
        "c": 1
      }
    },
    "field_1": {
      "1": {
        "c": 2
      }
    }
  }
}
```

There are two partitions in CubeDB, each with a single data point. There are two data points with the
field "field_1" with value "1".

It's possible to filter the data by a single field or multiple fields:

```shell
> curl http://localhost:9998/v1/test_cube/last/100?field_1=null
{
  "header": {
    "requestTs": 1508943342728,
    "processingTimeMs": 16,
    "request": {
      "field_1": [
        "null"
      ]
    },
    "params": {
      "range": [
        "100"
      ],
      "cubeName": [
        "test_cube"
      ]
    }
  },
  "response": {
    "p": {
      "partition-01": {
        "c": 0
      },
      "partition-02": {
        "c": 0
      }
    }
  }
}
```

We did not insert null values yet so there's nothing in both partitions.

## API

Currently CubeDB is operated via REST interface,
although it is possible to implement something quicker, once it becomes a problem.

CubeDB supports gzipped requests and responses.

### Inserting data

Data is inserted via HTTP POST:

`echo $data | curl -s --data-binary "@-" -H "Content-Type: text/json" -X POST http://localhost:9998/v1/insert`

$data itself is a json array of rows with metrics
```json
[
    {
        "counters": {
            "c": 20
        },
        "cubeName": "event_cube_109_day",
        "fields": {
            "app_version": "4.27.0",
            "auto_topup": 3,
            "brand": 2,
            "gender": 0,
            "has_spp": true,
            "is_default_product": true,
            "is_default_provider": true,
            "is_stored_method": true,
            "platform": 1
        },
        "partition": "2016-06-27"
    },
    {
        "counters": {
            "c": 1
        },
        "cubeName": "event_cube_109_day",
        "fields": {
            "app_version": "4.33.0",
            "auto_topup": 2,
            "brand": 3,
            "gender": 1,
            "has_spp": true,
            "is_default_product": true,
            "is_default_provider": true,
            "is_stored_method": true,
            "platform": 3
        },
        "partition": "2016-06-27"
    },
    {
        "counters": {
            "c": 1
        },
        "cubeName": "event_cube_109_day",
        "fields": {
            "app_version": "4.33.0",
            "auto_topup": 2,
            "brand": 3,
            "gender": 1,
            "has_spp": true,
            "is_default_product": true,
            "is_default_provider": true,
            "is_stored_method": true,
            "platform": 3
        },
        "partition": "2016-06-27"
    },
    {
        "counters": {
            "c": 6
        },
        "cubeName": "event_cube_100_hour",
        "fields": {
            "activation_place": null,
            "app_version": "4.39.0",
            "brand": 2,
            "gender": 1,
            "gift_button": 1,
            "message_first": false,
            "platform": 3
        },
        "partition": "2016-06-27 00"
    },
    {
        "counters": {
            "c": 64
        },
        "cubeName": "event_cube_100_hour",
        "fields": {
            "activation_place": null,
            "app_version": "4.43.1",
            "brand": 2,
            "gender": 2,
            "gift_button": 7,
            "message_first": true,
            "platform": 3
        },
        "partition": "2016-06-27 00"
    }
]
```

In first instance rows are augmented with fields not specified and inserted. If this row already
exists in the DB, counters are incremented by the values specified. So, technically speaking this is
an **upsert** rather then *insert*.

### Querying data

Retrieve data for all cubes from between partitions {fromPartition}/{toPartition}:

`curl -s --request GET --header 'Content-Type: application/json' http://localhost:9998/v1/all/from/{fromPartition}/to/{toPartition}`

Retrieve data for a given cube {cubeName} from between partitions {fromPartition}/{toPartition}:

`curl -s --request GET --header 'Content-Type: application/json' http://localhost:9998/v1/{cubeName}/from/{fromPartition}/to/{toPartition}`

Retrieve data for a given cube {cubeName} from the last {num} partitions:

`curl -s --request GET --header 'Content-Type: application/json' http://localhost:9998/v1/{cubeName}/last/{num}`

Retrieve data for a given cube {cubeName} from the last {num} partitions, where field {field_name}
has {field_value} value:

`curl -s --request GET --header 'Content-Type: application/json' http://localhost:9998/v1/{cubeName}/last/{num}/?{field_name}={field_value}`

Retrieve data for a given cube {cubeName} from the last {num} partitions, where data is grouped by field {field_name}:

`curl -s --request GET --header 'Content-Type: application/json' http://localhost:9998/v1/{cubeName}/last/{num}/group_by/{field_name}`


### Deleting data

Deleting happens via DELETE HTTP method. You can only remove enitre partitions or ranges of partitions.

`curl -s --request DELETE --header 'Content-Type: application/json' http://localhost:9998/v1/all/from/{fromPartition}/to/{toPartition}`

- `/v1/keep/last/{numPartitions}` would delete all but the last *numPartitions*
- `/v1/{cubeName}` would delete (drop) table {cubeName}
- `/v1/all/from/{fromPartition}/to/{toPartition}` deletes all partitions ranging in [fromPartition, toPartition] inclusively
- `/v1/{cubeName}/from/{fromPartition}/to/{toPartition}` deletes all partitions ranging in [fromPartition, toPartition] inclusively within a table

### Statistics and monitoring

`http://localhost:9998/v1/stats` will give you all sorts of technical information,
including the list of all tables and number of partitions for each of them and in total.
It will also tell you the approximate size occupied by data., number of partitions that can be found
in heap and off-heap (see next chapter for explanations).

### Saving and backup

Please bear in mind that data is not saved automatically on shutdown. You have to trigger saving manually.
On startup the data is loaded from the directory specified in the command line argument.

POST-ing to `http://localhost:9998/v1/save` will trigger a database dump to disk.

- each table is serialized into a separate file.
- files are gzipped
- save directory is specified on server startup as command line argument
- HTTP response of the this request will give you the full path where the dump is saved.
- data is serialized in it's internal, hoighly efficient binary format.
- **WARNING** dumps are meant just to survive the restarts and it is not guaranteed that they will be compatible withe the new versions of cubeDB.

POST-ing to `http://localhost:9998/v1/saveJSON` will dump whole database in human readible format.

- each table is serialized into a separate file.
- files are gzipped
- save directory is a subdirectory specified on server startup as command line argument
- HTTP response of the this request will give you the full path where the dump is saved.
- data is serialized in human readible, one-json-per-line format
- it should be compatible with newer versions of CubeDB.

## Installing and compiling


### Installation

**Requirements:** you need git, JDK 8 and Maven to be installed on your system

```shell
git clone git@github.com:sztanko/cubedb.git
cd cubedb/
mvn package -DskipTests
mv target/cubedb-*-SNAPSHOT.jar cubedb.jar
java -jar cubedb.jar <port> <path_for_dumps>
```

I recommend creating a `run.sh` file that would run it for you.

```shell
path="/tmp/cubedumps" # Directory for dumps
log_properties="log4j.properties" # Create your own log4.properties
port=9998 # port
jmx_port=5010 # monitoring port
flightRecordsOpts="-XX:+UnlockCommercialFeatures -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=$jmx_port"
logOpts="-Dlog4j.configuration=$log_properties"
/usr/java/jdk1.8.0_60/bin/java -XX:MaxDirectMemorySize=10G -Xmx2000M $flightRecordsOpts $logOpts -jar cubedb.jar $port $path
```

### Stopping the server

Just Ctrl-C the task and wait a little bit. It is advised to save the data before the shutdown.

### Running on Docker

It's possible to build and run CubeDB in a container using a supplied Dockerfile. Given a working
Docker server/client on the machine run the following in the root of the CubeDB repository:

```shell
docker build -t cubedb .
docker run -t -p 80:80 cubedb
```

CubeDB will then be available on the default HTTP port.

## Technical details

- current version of CubeDB is implemented in Java 8. You need to have it installed in order to process it.
- when querying the data, all CPU's are utilized in parallel
- a full scan of records is done for every query
- current scan performance is around 20 million records/CPU/second
- data is stored in columnar manner + a hashmap is used for record lookup (to check if the records is already is done)
- if the table is small (<4096 records), a column is stored in a Trove short array, metric is stored in a Trove long array
- columns exceeding 4096 records are stored off heap, using ByteBuffer.allocateDirect()
- Garbage collection is explicitely called after partitions are deleted. Garbage collection time is specified

** PRO TIP: *** if you are running this on SUN JDK, you can also use Java Mission Control to have some deep
introspection into what is exactly happenning in the

## Limitations

- dictionary compression is used for efficient storing of field names. The max cardinality of a field within one partition is 32677 thus.
- number of fields within one partition is limited to 256
- all data should fit into a memory, thus limiting number of aggregates to a billion
- *theoretically* the engine is capable of inserting 150 000 records / seconds in worst case scenario (no updates, inserts only), however the current bottleneck is HTTP interface and json deserialization cost anyway.


## Known bugs

- In some rare edge cases, CubeDB gives an 0 count of "null" value for a field for partitions which where created before this field.


[image_logo]: https://raw.githubusercontent.com/cubedb/cubedb/master/static/banner_horizontal.png
