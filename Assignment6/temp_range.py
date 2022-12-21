import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types



def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),])

    df = spark.read.csv(inputs, schema=observation_schema)

    correct_data = df.filter(df['qflag'].isNull()).cache()
    temperature_observations_tmax = correct_data.filter(correct_data['observation'] == 'TMAX').select('station', 'date', 'value')
    temperature_observations_tmin = correct_data.filter(correct_data['observation'] == 'TMIN').select('station', 'date', 'value')

    data_final = temperature_observations_tmax.alias('a').join(temperature_observations_tmin.alias('b'), on=['date', 'station']).select('a.date', 'a.station', functions.col('a.value').alias('tmax'), functions.col('b.value').alias('tmin'))
    # Dividing by 10 in below step
    range_df = data_final.withColumn('range', (data_final['tmax'] - data_final['tmin'])/10).select('date', 'station', 'range').cache()

    max_data = range_df.groupBy('date').agg(functions.max('range').alias('range'))

    outdata = range_df.alias('a').join(max_data.alias('b'), on=['date', 'range'], how='inner').select('a.date', 'a.station', 'a.range')

    outdata.sort('date', 'station').write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)