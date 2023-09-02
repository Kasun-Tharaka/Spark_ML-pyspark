from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


if __name__=='__main__':

    spark=SparkSession.builder.appName("demo").getOrCreate()

    person_list=[("sunny","","savita",1,"M"),
    ("bhavya","shah","",2,"M"),
    ("trilok","heisboy","best",7,"M")]

    schema=StructType([\
    StructField("firstname",StringType(),True),\
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", IntegerType(), True), \
    StructField("gender", StringType(), True), \
    ])

    data=spark.createDataFrame(data=person_list,schema=schema)

    data.printSchema()

    data.show()