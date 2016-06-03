from pyspark import SparkContext
from pyspark import SparkConf
from lib import LogLine
from lib import BootSession
import lib
import time
import ujson as json
import argparse

codec = 'org.apache.hadoop.io.compress.GzipCodec'
data_files = "hdfs://128.205.39.185:9000/devices/*/2016/*/*/*.out.gz"
out_dir = "hdfs://128.205.39.185:9000/processed"

def ll_mapper(line):
	ll = lib.logline_from_text(line)
	return [] if ll is None else [ll.convert_payload()]

def ll_gap_map(ll):
	return ((ll.boot_id, ll.timestamp[:10]), ll.line_num)
	#return ((ll.boot_id, ll.timestamp.strftime("%Y-%m-%d")), ll.line_num)

def find_gaps(t):
	((boot_id, date), lines) = t
	lines = sorted(lines)
	i = 1
	gaps = []

	while i < len(lines):
		if lines[i] - lines[i-1] > 2:
			gaps.append((boot_id, date, lines[i-1], lines[i], lines[i] - lines[i-1]))
		i += 1

	return gaps

#
# Find gaps in the data and persist as a collection of (parsed) loglines
#
def main():
	conf = SparkConf()
	conf.set("spark.default.parallelism", "24")
	sc = SparkContext(appName="PhoneLab Preprocessing", conf=conf)

	lines = sc.textFile(data_files, use_unicode=False)

	# Create LogLine objects and filter out empty lines
	logs = lines.flatMap(ll_mapper)

	# Save in an intermediate format
	logs.saveAsTextFile(out_dir, compressionCodecClass=codec)
	return

	# Gap detection
	keyed = logs.map(ll_gap_map)
	merged = keyed.groupByKey()

	# At this point we have ((boot_id, date), [line_num]) tuples The last step.
	# is to find all the gaps within each key/tuple.
	result = merged.flatMap(find_gaps)
	gaps = result.collect()

	fd = open("/spark/gaps.json", 'w')
	fd.write(json.dumps(gaps, indent=4))

if __name__ == '__main__':
	now = time.time()
	main()
	print "Operation took {}s".format(time.time()-now)
