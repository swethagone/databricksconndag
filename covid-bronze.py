from pyspark.sql.functions import schema_of_json,lit
from pyspark.sql.functions import from_json
from pyspark.sql.functions import *

df = spark.read.json("s3://experian-sdlf-dev-us-east-2-683819638661-raw/covid/dailycases=dailycases/")
schema = df.schema
dataschema = df.select(schema_of_json(df.select("data").first()[0])).collect()[0][0]

print(schema)
print(dataschema)



inputDf = spark.readStream.schema(schema).json("s3://experian-sdlf-dev-us-east-2-683819638661-raw/covid/dailycases=dailycases/")
print(df)
display(df)



dataDf = inputDf.selectExpr("CAST(data AS STRING) as json")\
                    .select( from_json("json", schema=dataschema).alias("data"))\
                    .select(dayofmonth(current_date()).alias("day"),
    month(current_date()).alias("month"),
    year(current_date()).alias("year"),"data.*")

display(dataDf)


dataDf.writeStream.partitionBy("year", "month", "day").format("delta").outputMode("append").option("checkpointLocation","s3n://experian-sdlf-dev-us-east-2-683819638661-stage/checkpoint/").option("path", "s3n://experian-sdlf-dev-us-east-2-683819638661-stage/post-stage/engineering/legislators/").start()
