from pyspark import SparkContext, SparkConf
import sys
import random


def map_func(data):
    
    iterations = 0

    random.seed()
    for i in data:
        sum = 0.0
        while sum < 1:
            sum += random.uniform(0,1)
            iterations+=1

    return iterations


def main(inputs):

    rdd_samples = sc.parallelize(range(int(inputs)),numSlices=50).glom()

    rdd_map = rdd_samples.map(map_func)

    rdd_reduce = rdd_map.reduce(lambda x, y: (x+y))

    print(rdd_reduce/int(inputs))


if __name__ == '__main__':
    conf = SparkConf().setAppName('Euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]

    main(inputs)

