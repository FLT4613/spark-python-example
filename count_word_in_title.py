from pyspark.sql import SparkSession

title_basics = "hdfs:/IMDB_data/title.basics.tsv"

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df_title = spark.read.csv(
    title_basics,
    header=True,
    sep='\t',
    encoding='utf-8'
).cache()

res = (
    df_title.select('primaryTitle')
    .rdd.flatMap(lambda x: x.primaryTitle.split(' '))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda (x, y): y, ascending=False)
    .collect()
)

for i, x in enumerate(res[:30], start=1):
    print(u'{:>2} : {} ({})'.format(i, x[0], x[1]))

spark.stop()
