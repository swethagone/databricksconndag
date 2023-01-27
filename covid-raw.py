kinesisDF = spark.readStream \
          .format("kinesis") \
          .option("streamName", "covid-data-caes-stream") \
          .option("region", "us-east-2") \
          .option("initialPosition", "EARLIEST") \
          .option("inferSchema", "true") \
          .load()

display(kinesisDF)


result = kinesisDF\
    .selectExpr('CAST(data AS STRING) data')

display(result)

from pyspark.sql.functions import *

result.select(
    dayofmonth(current_date()).alias("day"),
    month(current_date()).alias("month"),
    year(current_date()).alias("year"),
    lit("dailycases").alias("dailycases"),
       "data").writeStream.partitionBy("dailycases","year", "month", "day").format("json").outputMode("append").option("checkpointLocation","s3n://experian-sdlf-dev-us-east-2-683819638661-raw/checkpoint/").option("path","s3n://experian-sdlf-dev-us-east-2-683819638661-raw/covid/").start()
