from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def kv_func(json_data):
    subreddit = str(json_data['subreddit'])
    score = int(json_data['score'])
    return (subreddit, (1,score))


def add_pairs(x, y):
    final_count = x[0]+y[0]
    final_score = x[1]+y[1]
    return (final_count, final_score)


def avg(data):
    subreddit, count_score = data
    count, score = count_score
    average = score/count
    return (subreddit, average)


def bcast_relative_score(broadcast, data):
    subreddit, score_author = data
    # score_author is tuple of score and author
    return (score_author[0]/broadcast.value[subreddit], score_author[1])



def main(inputs, output):
    text = sc.textFile(inputs)
    json_load = text.map(json.loads).cache()

    kv_pair = json_load.map(kv_func)
    kv_reduced = kv_pair.reduceByKey(add_pairs)
    kv_avg_score = kv_reduced.map(avg).filter(lambda x: x[1]>0)

    commentbysub = json_load.map(lambda c: (str(c['subreddit']), (int(c['score']), c['author'])))
    broadcast = sc.broadcast(dict(kv_avg_score.collect()))

    outdata = commentbysub.map(lambda x: bcast_relative_score(broadcast, x))

    outdata.sortBy(lambda x: x[0], ascending=False).map(json.dumps).saveAsTextFile(output)



if __name__ == '__main__':
    conf = SparkConf().setAppName('relative_score_bcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
