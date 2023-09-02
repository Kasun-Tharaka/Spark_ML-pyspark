from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer


#start spark session
spark = SparkSession\
        .builder\
        .appName("DecisionTreeWithSpark")\
        .getOrCreate()


#load the data
dataset = spark.read.csv("/config/workspace/winequality_red.csv", header = True)

#dataset.show()

#dataset.printSchema()


from pyspark.sql.functions import col

#type casting, convert string columns into float
new_dataset = dataset.select(*(col(c).cast("float").alias(c) for c in dataset.columns))
new_dataset.printSchema()


from pyspark.sql.functions import col, count, isnan, when

#chech for null values
new_dataset.select([count(when(col(c).isNull(),c)).alias (c) for c in new_dataset.columns]).show()
cols = new_dataset.columns

#remove the target variable
cols.remove("quality")

#transform columns into vectors
assembler = VectorAssembler(inputCols = cols, outputCol = "features")
data = assembler.transform(new_dataset)

data = data.select("features", "quality")

data.show()


from pyspark.ml.feature import StringIndexer

#crate indexes foe target variable
stringIndexer = StringIndexer(inputCol = "quality", outputCol = "quality_index")

data_indexed = stringIndexer.fit(data).transform(data)

data_indexed.show()

#split the data
(train,test) = data_indexed.randomSplit([0.7,0.3])


#model building
dt = DecisionTreeClassifier(labelCol = "quality_index", featuresCol = "features")
model=dt.fit(train)

#prediction
prediction=model.transform(test)

#for showcase the prediction
prediction.show()

#evalutor
evaluator = MulticlassClassificationEvaluator(labelCol = "quality_index", predictionCol = "prediction", metricName = "accuracy")
accuracy = evaluator.evaluate(prediction)

print("Accuracy", accuracy)

#saving the model
#model.save("decisiontree")

model.save("decisiontree.pkl")

#load the model
model.load("")

#spark.stop()