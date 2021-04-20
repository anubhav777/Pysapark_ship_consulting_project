import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.appName('Hyundai').getOrCreate()

df = spark.read.csv('cruise_ship_info.csv',inferSchema=True,header=True)

df.printSchema()

indexer = StringIndexer(inputCol='Cruise_line',outputCol='Crus_ln')
indexed = indexer.fit(df)
data=indexred.transform(df)

asssembler = VectorAssembler(inputCols =['Crus_ln','Age','Tonnage','passengers','length','cabins','passenger_density'],outputCol='features')

output = asssembler.transform(data)

final_data=output.select('features','crew')

train_data,test_data = final_data.randomSplit([0.7,0.3])

lr = LinearRegression(labelCol='crew')

lr_model = lr.fit(train_data)

test_result = lr_model.evaluate(test_data)

print(test_result.rootMeanSquaredError)

print(test_result.r2)