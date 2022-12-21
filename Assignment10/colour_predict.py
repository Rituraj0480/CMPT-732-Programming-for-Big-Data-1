import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):

    # getting data and splitting it into training and testing dataframes
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # building steps for pipeline
    rgb_assembler = VectorAssembler(inputCols = ['R', 'G', 'B'], outputCol = "features")
    word_indexer = StringIndexer(inputCol = "word", outputCol="target")
    classifier = MultilayerPerceptronClassifier(featuresCol = "features", labelCol = "target", maxIter=400, layers=[3, 35, 11])

    
    # create a pipeline to predict RGB colours -> word
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    
    # predictions
    predictions = rgb_model.transform(validation)
    #predictions.show()

    # create an evaluator and score the validation data
    evaluator = MulticlassClassificationEvaluator(predictionCol = 'prediction', labelCol = 'target')

    score = evaluator.evaluate(predictions)
    print('Validation score for RGB model: %g' % (score, ))
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    
    
    # create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)

    rgb_lab_assembler = VectorAssembler(inputCols = ['labL', 'labA', 'labB'], outputCol = "features")
    rgb_lab_pipeline = Pipeline(stages=[sqlTrans, rgb_lab_assembler, word_indexer, classifier])

    lab_model = rgb_lab_pipeline.fit(train)
    predictions_lab = lab_model.transform(validation)

    score_lab = evaluator.evaluate(predictions_lab)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', score_lab)

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
