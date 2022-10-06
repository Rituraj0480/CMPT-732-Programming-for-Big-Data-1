from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def func(line):

    record = json.loads(line)

    name = str(record['subreddit'])

    score = int(record['score'])

    return (name, (1,score))


def add_pairs(x, y):

    final_count = x[0]+y[0]
    final_score = x[1]+y[1]
    return (final_count, final_score)


def avg(data):
    name, count_score = data
    count, score = count_score
    average = score/count
    return (name, average)


def print_format(data):
    return json.dumps(data)


def main(inputs, output):
    #loading dataset
    text = sc.textFile(inputs)

    record = text.map(func)

    print(record.take(10))  #[('xkcd', (1, 0)), ('xkcd', (1, 3)), ('Genealogy', (1, 4)), ('xkcd', (1, 2)), ('scala', (1, 1)), ('xkcd', (1, 2)), ('xkcd', (1, 2)), ('xkcd', (1, 3)), ('xkcd', (1, 7)), ('xkcd', (1, 19))]

    record_2 = record.reduceByKey(add_pairs)

    print(record_2.take(10))    #[('scala', (971, 1873)), ('Genealogy', (746, 1396)), ('xkcd', (7269, 38329)), ('Cameras', (9, 11)), ('optometry', (151, 222))]

    record_3 = record_2.map(avg)

    outdata = record_3.sortBy(lambda x: x[0])

    outdata.map(print_format).saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
