# s3-access-logs-parser

Spark application to parser access logs and arrange them for better access

## How to run?

```shell
spark-submit --deploy-mode cluster --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
             --conf spark.sql.caseSensitive=true --class me.rotemfo.spark.accesslog.S3AccessLogsApp \
             s3://seekingalpha-data/accessories/jars/s3-access-logs-assembly.jar --input-path s3://s3-access-logs/ --prefix emr \
             --input-datetime 2021-11-08-00 --output-path s3://formatted-s3-access-logs/s3-access-logs/ --partitions 4
```