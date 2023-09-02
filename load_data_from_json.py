from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("readdatajson").getOrCreate()

dataframe=spark.read.json("/config/workspace/demojson2.json")

print(type(dataframe))

dataframe.printSchema()

dataframe.show()
