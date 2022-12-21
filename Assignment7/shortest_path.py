from importlib.resources import path
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
import re


def kv_pair(line):
    list_arr = line.split(':')
    s_node = int(list_arr[0])
    d_nodes = list_arr[1].split(' ')
    for i in d_nodes:
        if i!="":
            yield (s_node, int(i))

# [(1, 3), (1, 5), (2, 4), (3, 2), (3, 5), (4, 3), (6, 1), (6, 5)]

def min_distance(x, y):
    source1, distance1 = x
    source2, distance2 = y
    if distance1<=distance2:
        return x
    else:
        return y


def main(inputs, output, source, destination):

    text = sc.textFile(inputs + '/links-simple-sorted.txt')
    graph_edges = text.flatMap(kv_pair).cache()
    known_paths = sc.parallelize([(1, ('',0))])

    for i in range(6):
        joined = graph_edges.join(known_paths.filter(lambda x: x[1][1]==i))
        #[(1, (3, ('', 0))), (1, (5, ('', 0)))]
        next_nodes = joined.map(lambda x: (x[1][0], (x[0], i+1)))
        known_paths = known_paths.union(next_nodes).reduceByKey(min_distance).cache()

        known_paths.saveAsTextFile(output + '/iter-' + str(i))
        if len(known_paths.filter(lambda x: x[0]==destination).collect())>=1:
            break

    # print(known_paths.collect()) #[(1, ('', 0)), (2, (3, 2)), (3, (1, 1)), (4, (2, 3)), (5, (1, 1))]

    path_list = [destination]
    dest = destination
    while(dest!=source):
        dest_lookup = known_paths.lookup(dest)
        if not dest_lookup:
            path_list = ['invalid destination']
            break
        dest = dest_lookup[0][0]
        path_list.append(dest)
        
    path_list.reverse()

    finalpath = sc.parallelize(path_list)
    finalpath.saveAsTextFile(output + '/path')
    #print(finalpath.collect())


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, destination)