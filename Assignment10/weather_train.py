import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main(input, output):
    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
        ])

    data = spark.read.csv(input, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # sqlTrans = SQLTransformer(statement = "SELECT latitude, longitude, elevation, dayofyear(date) AS dayofyear, tmax FROM __THIS__")
    # vecAssembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'dayofyear'], outputCol="features")
    # gbt = GBTRegressor(featuresCol="features", labelCol="tmax", maxIter=100)
    # pipeline = Pipeline(stages=[sqlTrans, vecAssembler, gbt])
    # gbt_model = pipeline.fit(train)

    # #create an evaluator and score the validation data
    # predictions = gbt_model.transform(validation)

    # r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    # r2 = r2_evaluator.evaluate(predictions)
    # print('\nr2 = ',r2)
    # rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    # rmse = rmse_evaluator.evaluate(predictions)
    # print('\n\nrmse = ',rmse)

    # gbt_model.write().overwrite().save(output)


    ## Feature Engineering
    statement = "SELECT today.latitude, today.longitude, today.elevation, dayofyear(today.date) AS dayofyear, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"
    sqlTrans = SQLTransformer(statement = statement)
    vecAssembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'dayofyear', 'yesterday_tmax'], outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol="tmax", maxIter=100)
    pipeline = Pipeline(stages=[sqlTrans, vecAssembler, gbt])
    gbt_model = pipeline.fit(train)


    #create an evaluator and score the validation data
    predictions = gbt_model.transform(validation)

    
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('\nr2 = ',r2)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('\n\nrmse = ',rmse)
    
    gbt_model.write().overwrite().save(output)

    
if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input, output)
