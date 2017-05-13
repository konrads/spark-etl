spark-etl
==========

Tooling for configuration and SQL transform driven Spark ETLs.

Build status (master): [![Build Status](https://travis-ci.org/konrads/spark-etl.svg?branch=master)](https://travis-ci.org/konrads/spark-etl)

Philosophy
----------
This library facilitates productionizing of configuration/SQL driven Spark ETL pipelines. Emphasis is on:
* configuration and SQLs treated as first class citizens
* build time validation comprising syntactical checks of config and SQL, ensuring that SQL datasources map to configured `extract`s and `transform`s
* run time validations comprising verification of data source (`extract`s) uris and connectivity to [LoadWriter](src/main/scala/spark_etl/LoadWriter.scala)
* optional validation of `extract` datasources
* optional validation of `transform` outputs (pre `load` writing)
* config and SQL parametrization via `${var}` style variables, configured ar runtime via `-Denv.var=value`
* CLI support for commands: `validate-local`, `validate-remote`, `extract-check`, `transform-load`, `transform-load`

Sample setup
------------
Setup `src/main/resources/app.yaml`:
```
extracts:
  - name:  client
    uri:   "hdfs://${path}/client_2017"
    check: "/spark/extract-check/client.sql"
  - name:  item
    uri:   "hdfs://${path}/item_2017"
  - name:  transaction
    uri:   "hdfs://${path}/transaction_2017"

transforms:
  - name:  client_spending
    sql:   "/spark/transform/client_spending.sql"
  - name:  item_purchase
    sql:   "/spark/transform/item_purchase.sql"
  - name:  minor_purchase
    check: "/spark/transform-check/minor_purchase.sql"
    sql:   "/spark/transform/minor_purchase.sql"

loads:
  - name:   client_spending_out
    source: client_spending
    uri:    "hdfs://out/client_spending"
    # no partition_by
  - name:   item_purchase_out
    source: item_purchase
    uri:    "hdfs://out/item_purchase"
    # no partition_by
  - name:   minor_purchase_out
    source: minor_purchase
    uri:    "hdfs://out/minor_purchase"
    # no partition_by

# optional extract_reader/load_writer
load_writer:
  class: "spark_etl.OracleLoadWriter"
  params:
    oracle_uri:          ${oracle_uri}
    oracle_user:         ${oracle_user}
    oracle_password:     ${oracle_password}
```

Setup your SQLs as per below. All SQLs are `SELECT` statements, `transform`s produce potentially sizable `Dataframes` to be persisted as `load`s, `extract-check` and `transform-check` produce smaller `Dataframees` which are loged out for visual inspection:
```
src -+
     |
     +- spark
          |
          +- extract-check
          |    |
          |    +- client.sql            # NOTE: optional extract validation!
          |
          +- transform
          |    |
          |    +- client_spending.sql
          |    |
          |    +- item_purchase.sql
          |    |
          |    +- minor_purchase.sql
          |
          +- transform-check
               |
               +- minor_purchase.sql   # NOTE: optional transform validation!
```
Validate local config/SQLs. Suggested use is to run this as part of the build, with validation failure stopping the build:
```
sbt "run-main spark_etl.Main -Denv.path=some_path validate-local"
```

Deploy to cluster, with read access to `hdfs://some_path`, write access to `hdfs://out`. If using yarn, utilize: [run.sh](src/main/resources/run.sh)
```
run.sh -Denv.path=some_path validate-remote
```

Run extract and transform validations on the cluster:
```
run.sh -Denv.path=some_path extract-check
run.sh -Denv.path=some_path transform-check
```

Run transformation and persist loads:
```
run.sh -Denv.path=some_path transform-load
```

If env `PACKAGE_LOGS=true`, `run.sh`'s cluster operations (`transform-load`, `extract-check`, `transform-check`) capture both driver and yarn logs under `logs/$app_id/logs_$app_id.zip`.
