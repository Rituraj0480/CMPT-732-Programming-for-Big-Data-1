from math import sqrt
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import re

def re_func(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    list_arr = line_re.split(line)
    if len(list_arr)==6:
        return (list_arr[1], int(list_arr[4]))


def main(inputs, output):
    schema = types.StructType([
    types.StructField('host_name', types.StringType()),
    types.StructField('bytes', types.IntegerType()),])

    text = sc.textFile(inputs)
    rdd_split = text.map(re_func).filter(lambda list_arr: list_arr is not None)
    df = rdd_split.toDF(schema)
    df_final = df.groupBy('host_name').agg(functions.count('bytes').alias('count_requests'), functions.sum('bytes').alias('sum_request_bytes'))
    
    df_final.createTempView("DF_FINAL")
    corr_df = spark.sql(" SELECT 1 AS ONE, count_requests AS X, sum_request_bytes AS Y, count_requests * count_requests AS X_2, sum_request_bytes * sum_request_bytes AS Y_2, count_requests * sum_request_bytes AS X_Y FROM DF_FINAL ")
    s = corr_df.groupBy().sum().collect()

    r = (s[0][0] * s[0][5] - s[0][1] * s[0][2])/(sqrt(s[0][0] * s[0][3] - s[0][1] * s[0][1]) * sqrt(s[0][0] * s[0][4] - s[0][2] * s[0][2]))
    print("The value of r = ", r)
    print("The value of r^2 = ", r*r)



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)