# parquet-writer

Converts [TPC-DS](https://www.tpc.org/tpcds/) .dat files [IMDB](https://datasets.imdbws.com/) .tsv files into [.parquet files](https://parquet.apache.org/) using [Apache Parquet Java](https://github.com/apache/parquet-java). 

TPC-DS schema information is hardcoded in `TpcDsTableInfoUtil.java` since the Java API is unable to auto-detect columns or any other schema information. IMDB schema information is in `ImdbTableInfoUtil.java`.

Usage: `mvn exec:java@run -Dexec.args="{imdb|tpcds} {path to input dir} {path to output dir} [snappy|gzip|lzo] [--perf]"`

`--perf` will emit compression + decompression runtimes. This means making a read pass on all output `.parquet` files. 