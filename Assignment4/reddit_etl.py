from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def load_func(line):
    # function to get subreddit, score, and author

    subreddit = str(line['subreddit'])

    score = int(line['score'])

    author = str(line['author'])

    return (subreddit, score, author)


def main(inputs, output):

    text = sc.textFile(inputs).map(json.loads)

    record = text.map(load_func)

    filter_record = record.filter(lambda x: 'e' in x[0].lower())

    filter_record.cache()

    filter_record.filter(lambda x: x[1]>0).map(json.dumps).saveAsTextFile(output + '/positive')

    filter_record.filter(lambda x: x[1]<=0).map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
