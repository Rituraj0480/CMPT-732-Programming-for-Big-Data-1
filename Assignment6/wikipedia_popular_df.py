from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
	Datetime = path.split('/')[-1]
	Date = Datetime.split('-')[1]
	hour = Datetime.split('-')[2][0:2]
	return Date+'-'+hour

def main(inputs, output):
    schema = types.StructType([
    types.StructField('lan', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('count', types.IntegerType()),
    types.StructField('bytes', types.StringType()),
])

    df = spark.read.csv(inputs, schema=schema, sep=' ').withColumn('filename', functions.input_file_name())

    wiki2 = df.select('lan', 'title', 'count', path_to_hour('filename').alias('hour'))

    wiki3 = wiki2.filter( (wiki2['lan']=="en") & (wiki2['title']!="Main_Page") & ( ~ wiki2['title'].startswith("Special:")) ).cache()

    max_views = wiki3.groupBy(wiki3['hour']).agg(functions.max(wiki3['count']).alias('views')).alias('max_views')

    outdata = wiki3.alias('wiki3').join(max_views, [wiki3['hour']==max_views['hour'], wiki3['count']==max_views['views']], 'inner').select('wiki3.hour', 'wiki3.title', 'max_views.views')

    # outdata = wiki3.alias('wiki3').join(max_views.hint("broadcast"), [wiki3['hour']==max_views['hour'], wiki3['count']==max_views['views']], 'inner').select('wiki3.hour', 'wiki3.title', 'max_views.views')

    outdata.explain()
    outdata.sort(['hour', 'title']).write.json(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)


