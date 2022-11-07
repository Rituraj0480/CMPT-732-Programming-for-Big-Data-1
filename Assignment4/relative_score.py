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


def relative_score_func(data):
    subreddit, tuple_data = data
    score_author, avg_score = tuple_data
    return (score_author[0]/avg_score, score_author[1])


def main(inputs, output):

    text = sc.textFile(inputs)
    json_load = text.map(json.loads)
    json_load.cache()

    record = json_load.map(kv_func)
    record_2 = record.reduceByKey(add_pairs)
    record_3 = record_2.map(avg).filter(lambda x: x[1]>0).cache()

    commentbysub = json_load.map(lambda c: (str(c['subreddit']), (int(c['score']), str(c['author']))))
    commentbysub_2 = commentbysub.join(record_3)

    # print(commentbysub_2.take(10))
    #[('MechanicalKeyboards', ((1, 'diosim'), 2.2637818544562363)), ('MechanicalKeyboards', ((2, 'Vermiliion'), 2.2637818544562363)), ('MechanicalKeyboards', ((2, 'coldpizzaa'), 2.2637818544562363)), ('MechanicalKeyboards', ((2, 'adamstributer'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, '[deleted]'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, 'sorijealut'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, '[deleted]'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, 'Whales96'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, 'DrTee83'), 2.2637818544562363)), ('MechanicalKeyboards', ((1, 'r3Fuze'), 2.2637818544562363))]

    commentbysub_3 = commentbysub_2.map(relative_score_func)
    commentbysub_3.sortBy(lambda x: x[0], ascending=False).map(json.dumps).saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative_score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
