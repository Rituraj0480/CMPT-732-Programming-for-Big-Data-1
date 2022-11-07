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

    correct_data = df.filter(df['qflag'].isNull())
    Canadian_data = correct_data.filter(correct_data['station'].startswith('CA'))
    temperature_observations = Canadian_data.filter(Canadian_data['observation'] == 'TMAX')
    df_new = temperature_observations.withColumn('tmax', temperature_observations['value']/10)

    etl_data = df_new.select('station', 'date', 'tmax')

    etl_data.write.json(output, compression='gzip', mode='overwrite')

    # print(etl_data.filter(etl_data['station']=='CA001039035').show(10))

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)