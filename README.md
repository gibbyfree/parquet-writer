# tpc-ds-parquet

Converts [TPC-DS](https://www.tpc.org/tpcds/) .dat files into [.parquet files](https://parquet.apache.org/) using [Apache Parquet Java](https://github.com/apache/parquet-java). TPC-DS schema information is hardcoded in `TableInfoUtil.java` since the Java API is unable to auto-detect columns or any other schema information.

Usage: `mvn exec:java@run -Dexec.args="{path to .dat dir} {output dir} [snappy|gzip|lzo] [--perf]"`

`--perf` will emit compression + decompression runtimes. This means making a read pass on all output `.parquet` files. 