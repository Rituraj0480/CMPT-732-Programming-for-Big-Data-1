from math import sqrt
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
import re


def main(input_keyspace, table):
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=input_keyspace).load()
    df_final = df.groupBy('host').agg(functions.count('bytes').alias('count_requests'), functions.sum('bytes').alias('sum_request_bytes'))
    
    df_final.createTempView("DF_FINAL")
    corr_df = spark.sql(" SELECT 1 AS ONE, count_requests AS X, sum_request_bytes AS Y, count_requests * count_requests AS X_2, sum_request_bytes * sum_request_bytes AS Y_2, count_requests * sum_request_bytes AS X_Y FROM DF_FINAL ")
    s = corr_df.groupBy().sum().collect()

    r = (s[0][0] * s[0][5] - s[0][1] * s[0][2])/(sqrt(s[0][0] * s[0][3] - s[0][1] * s[0][1]) * sqrt(s[0][0] * s[0][4] - s[0][2] * s[0][2]))
    print("The value of r = ", r)
    print("The value of r^2 = ", r*r)



if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    table = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Cassandra_correlate_logs') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_keyspace, table)