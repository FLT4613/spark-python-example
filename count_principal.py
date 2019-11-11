from pyspark.sql import SparkSession
import sys
name_basics = "hdfs:/IMDB_data/name.basics.tsv"
title_basics = "hdfs:/IMDB_data/title.basics.tsv"
title_principals = "hdfs:/IMDB_data/title.principals.tsv"

if len(sys.argv) != 2:
    exit(1)

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df_principals = spark.read.csv(
    title_principals,
    header=True,
    sep='\t',
    encoding='utf-8'
).cache()

df_name = spark.read.csv(
    name_basics,
    header=True,
    sep='\t',
    encoding='utf-8'
).cache()

df_title = spark.read.csv(
    title_basics,
    header=True,
    sep='\t',
    encoding='utf-8'
).cache()

"""
+------------+
|   titleType|
+------------+
|    tvSeries|
|tvMiniSeries|
|     tvMovie|
|   tvEpisode|
|       movie|
|   tvSpecial|
|       video|
|   videoGame|
|     tvShort|
|       short|
+------------+
"""
df_title_principals = (
    df_title
    .filter(df_title.titleType == sys.argv[1])
    .join(df_principals, df_title.tconst == df_principals.tconst)
)
res = (
    df_title_principals
    .join(df_name, df_title_principals.nconst == df_name.nconst)
    .groupBy('primaryName').count().sort('count', ascending=False)
)

res.show(10)
spark.stop()
