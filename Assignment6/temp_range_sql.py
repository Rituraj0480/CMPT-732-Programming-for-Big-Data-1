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

    # correct_data = df.filter(df['qflag'].isNull()).cache()
    df.createTempView("DF")
    correct_data = spark.sql(" SELECT * FROM DF WHERE QFLAG IS NULL ").cache()
    correct_data.createTempView("CORRECT_DATA")
    temp_obs_tmax = spark.sql(" SELECT DATE, STATION, VALUE AS TMAX FROM CORRECT_DATA WHERE observation='TMAX' ")
    temp_obs_tmin = spark.sql(" SELECT DATE, STATION, VALUE AS TMIN FROM CORRECT_DATA WHERE observation='TMIN' ")
    temp_obs_tmax.createTempView("TEMP_OBS_TMAX")
    temp_obs_tmin.createTempView("TEMP_OBS_TMIN")

    temp_obs_2 = spark.sql(" SELECT DATE, STATION, (TMAX-TMIN)/10 AS RANGE FROM (SELECT A.DATE, A.STATION, A.TMAX, B.TMIN FROM TEMP_OBS_TMAX A INNER JOIN TEMP_OBS_TMIN B ON A.DATE=B.DATE AND A.STATION=B.STATION) ").cache()
    temp_obs_2.createTempView("TEMP_OBS_2")
    max_range = spark.sql(" SELECT DATE, MAX(RANGE) AS MAX_RANGE FROM TEMP_OBS_2 GROUP BY DATE ")
    max_range.createTempView("MAX_RANGE")
    outdata = spark.sql(" SELECT A.DATE, A.STATION, A.RANGE FROM TEMP_OBS_2 A INNER JOIN MAX_RANGE B ON (A.DATE=B.DATE AND A.RANGE=B.MAX_RANGE) ORDER BY DATE, STATION ")
    outdata.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)