from pyspark import SparkContext, SparkConf
import sys


#input & output
inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia_popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'


def func(line):
	string_arr = line.split()

	datetime = string_arr[0]
	lan = string_arr[1]
	name = string_arr[2]
	cnt = int(string_arr[3])

	return (datetime,lan,name,cnt)


def maximum(x,y):
	if x[0]>=y[0]:
		return x
	else:
		return y


def get_key(kv):
	return kv[0]


def tab_separated(kv):
	k,v = kv
	return "%s\t%s" % (k,v)


text = sc.textFile(inputs)

words = text.map(func)

words2 = words.filter(lambda x: x[1]=="en" and x[2] != "Main_Page" and not (x[2].startswith("Special:")))

pairs = words2.map(lambda x: (x[0],(x[3], x[2])))

redu = pairs.reduceByKey(maximum)

outdata = redu.sortBy(get_key)
outdata.map(tab_separated).saveAsTextFile(output)

print(outdata.take(10))