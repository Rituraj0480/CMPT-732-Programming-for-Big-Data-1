import sys, uuid, re
from datetime import datetime
from pyspark.sql import SparkSession, functions, types

def re_func(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    list_arr = line_re.split(line)
    if len(list_arr)==6:
        return (str(uuid.uuid4()), list_arr[1], datetime.strptime(list_arr[2], '%d/%b/%Y:%H:%M:%S'), list_arr[3], int(list_arr[4]))


def main(input_dir, output_keyspace, output_table):
    schema = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.TimestampType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType()),])

    text = sc.textFile(input_dir)
    rdd_split = text.map(re_func).filter(lambda data: data is not None)
    df = rdd_split.toDF(schema)
    df.write.format("org.apache.spark.sql.cassandra").options( table = output_table, keyspace = output_keyspace).save(mode="append")




if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_keyspace = sys.argv[2]
    output_table = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load_logs_spark') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, output_keyspace, output_table)