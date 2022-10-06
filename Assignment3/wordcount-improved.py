from pyspark import SparkConf, SparkContext
import sys
import re
import string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

    for w in wordsep.split(line):
        yield (w.lower(), 1)    # yield is used with loops and flatmap


def add(x, y):
    return x + y


def get_key(kv):
    return kv[0]


def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def main(inputs, output):
    text = sc.textFile(inputs).repartition(40)
    # Original Partitions = 8

    words = text.flatMap(words_once)

    words_2 = words.filter(lambda w: len(w[0])>0)
    wordcount = words_2.reduceByKey(add)

    outdata = wordcount.map(output_format)

    print("Output Data: {}".format(outdata.take(10)))
    
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)