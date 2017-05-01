spark-etl
=========

Build status (master): [![Build Status](https://travis-ci.org/konrads/spark-etl.svg?branch=master)](https://travis-ci.org/konrads/spark-etl)

Set of ETL utils for spark:
* provides SQL and configuration driven ETL pipelines, treating scala code, SQL and yaml configuration as first class citizens. Emphasis is on:
  * Single configuration for extracts, transforms, ExtractReader, LoadWriter, eg. spark-etl-demo's [app.yaml](https://github.com/konrads/spark-etl-demo/blob/master/src/main/resources/app.yaml)
    * Token substitution in configuration, replacing config's `${var}` with params provided in CLI's `-Denv.var=value`
  * Local validation (eg. build time)
    * extract specification, including "check" SQL
    * transform specification, including transform and "check" SQL
    * validation of any SQLs: extract's "check", transform's transform and "check"
    * matching SQLs' data sources to extracts
    * pluggable extract_reader and load_writer local validations, eg. checking LoadWriter artifacts 
  * Remote validation (eg. prior to running a spark job)
    * extract read validation
    * post transform validation
    * pluggable extract_reader and load_writer local validations, eg. checking LoadWriter connection
  * Pluggable ExtractReader (defaulting to ParquetExtractReader)
  * Pluggable LoadWriter (defaulting to ParquetLoadWriter)
  * CLI support via [Main](src/main/scala/spark_etl/Main.scala), and its building blocks [MainUtils](src/main/scala/spark_etl/MainUtils.scala)
  * SBT support via [MainSbt](src/main/scala/spark_etl/MainSbt.scala), for build time validations
  
Validations are gathered and reported in bulk.
