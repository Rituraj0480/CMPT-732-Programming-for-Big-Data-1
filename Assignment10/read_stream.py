import sys
from pyspark.sql import SparkSession, functions


def main(topic):
    
    # getting data from topic into dataframe
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
            .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    # getting the required columns
    df = values.withColumn('x', functions.split(values['value'], ' ').getItem(0)).withColumn('y', functions.split(values['value'], ' ').getItem(1)).select('x', 'y')
    df1 = df.withColumn('xy', df['x']*df['y']).withColumn('x_2', df['x']*df['x']).withColumn('one', functions.lit(1))
    final_df = df1.agg(functions.sum(df['x']).alias('sum_x'), functions.sum(df['y']).alias('sum_y'), functions.sum("xy").alias("sum_xy"), functions.sum("x_2").alias("sum_x_2"), functions.sum("one").alias("n"))
    
    # calculating the slope and intercept 
    beta = (final_df['sum_xy'] - final_df['sum_x']*final_df['sum_y']/final_df['n']) / (final_df['sum_x_2'] - final_df['sum_x']*final_df['sum_x'] / final_df['n'])
    alpha = (final_df['sum_y'] / final_df['n'] - beta * final_df['sum_x'] / final_df['n'])

    # creating final output dataframe with single row
    output_df = final_df.withColumn('alpha', alpha).withColumn('beta', beta).select("alpha", "beta")
    stream = output_df.writeStream.format("console").outputMode('update').start()
    stream.awaitTermination(600)

if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)