import sys
from datetime import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):

    input = [('SFU', datetime.strptime('2022-11-19', '%Y-%m-%d'), 49.2771, -122.9146, 330.0, 0.0),
                ('SFU', datetime.strptime('2022-11-18', '%Y-%m-%d'), 49.2771, -122.9146, 330.0, 12.0)]

    sfu_tmax = spark.createDataFrame(input, schema=tmax_schema)
    sfu_tmax.show()

    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(sfu_tmax)
    prediction = predictions.select("prediction").collect()[0][0]
    print('Predicted tmax tomorrow:', prediction)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
