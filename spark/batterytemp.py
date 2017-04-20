from pyspark import SparkContext
from pyspark import SparkConf
from lib import LogLine
from lib import BatteryInterval
import lib
import time
from collections import namedtuple
from pyspark.rdd import portable_hash

data_files = "/spark/data/devices/1a28ea49f4206010fee054f9bdb86f822dc4dd28/2016/05/02/*.out.gz"
processed_dir = "hdfs://128.205.39.185:9000/processed"
tags = ['Kernel-Trace', 'KernelPrintk']

def ll_mapper(line):
	ll = lib.logline_from_text(line)
	return [] if ll is None else [ll.convert_payload()]

def do_sort(e):
	return (e[0], sorted(e[1], key=lambda x: x.line_num))

def process(group):
	# Battery state machine
	STATE_WAIT_FOR_DISCHARGE = 0
	STATE_WAIT_BATTERY_CHANGE = 1
	STATE_WAIT_FOR_RESUME = 2
	STATE_PROCESSING = 3
	STATE_DISCARD = 4

	# When we start out, we need to wait until we know we're discharging
	cur_state = STATE_WAIT_FOR_DISCHARGE

	# Log lines we care about
	LOG_MISC = 0
	LOG_BATTERY = 1
	LOG_TEMP = 2
	LOG_SUSPEND = 3
	LOG_CANCEL_SUSPEND = 4
	LOG_RESUME = 5

	# Log line variables
	cur_type = LOG_MISC
	new_level = 0
	discharging = None
	new_temp = 0

	# Results
	results = []
	cur_temps = []

	# State
	cur_level = 0
	first_line = 0
	start_time = 0.0
	prev_boot_id = ''

	for item in group:
		end = False
		discard = False
		ll = item[1]

		if ll.boot_id != prev_boot_id:
			prev_boot_id = ll.boot_id
			cur_state = STATE_WAIT_FOR_DISCHARGE

		# Decode the logline
		cur_type = LOG_MISC
		try:
			if ll.tag == 'KernelPrintk':
				if ll.pk_type == lib.PRINTK_HEALTHD:
					discharging = len(ll.batt_chg.strip()) == 0
					new_level = ll.batt_level
					cur_type = LOG_BATTERY
				elif ll.pk_type == lib.PRINTK_FREEZING:
					cur_type = LOG_SUSPEND
				elif ll.pk_type == lib.PRINTK_FREEZE_ABORT:
					cur_type = LOG_CANCEL_SUSPEND
				elif ll.pk_type == lib.PRINTK_RESTART_TASKS:
					cur_type = LOG_RESUME
			elif ll.tag == 'Kernel-Trace' and ll.trace_event == 'thermal_temp':
				new_temp = ll.temp
				cur_type = LOG_TEMP
		except:
			# Malformed tuple/logline
			continue

		# At the top-level, we discard everything that intersects a suspend window.
		if cur_type == LOG_SUSPEND:
			cur_state = STATE_WAIT_FOR_RESUME
			continue
		elif cur_type == LOG_RESUME or cur_type == LOG_CANCEL_SUSPEND:
			# FIXME: For cancel suspend, we could just save the output in a temp
			# list and commit them if we see the cancel event.
			cur_state = STATE_WAIT_FOR_DISCHARGE
			continue

		# Next, handle charging state. If the battery is not discharging, or we don't know
		# we need to wait until we do know.
		if not discharging and cur_state != STATE_WAIT_FOR_DISCHARGE:
			cur_state = STATE_WAIT_FOR_DISCHARGE
		if not discharging:
			continue
		elif cur_state == STATE_WAIT_FOR_DISCHARGE:
			if cur_type == LOG_BATTERY:
				# We don't know where within the battery level we are,
				# so, wait until it drops to the next level.
				cur_state = STATE_WAIT_BATTERY_CHANGE
				cur_level = new_level
		elif cur_state == STATE_WAIT_BATTERY_CHANGE:
			if cur_type == LOG_BATTERY:
				if new_level == cur_level - 1:
					cur_level = new_level
					cur_state = STATE_PROCESSING
					cur_temps = []
					first_line = ll.line_num
					start_time = ll.pk_timestamp
				elif new_level != cur_level:
					# Bad data
					cur_state = STATE_WAIT_FOR_DISCHARGE
		elif cur_state == STATE_PROCESSING:
			if cur_type == LOG_BATTERY:
				if new_level == cur_level - 1:
					# The level dropped, create a data point for the interval
					interval = BatteryInterval(cur_level, cur_temps, ll.boot_id, first_line, ll.line_num, start_time, ll.pk_timestamp)
					results.append(interval)
					cur_temps = []
					first_line = ll.line_num
					start_time = ll.pk_timestamp
					cur_level = new_level
				elif ll.batt_level != cur_level:
					# Bad data
					cur_state = STATE_WAIT_FOR_DISCHARGE
			elif cur_type == LOG_TEMP:
				cur_temps.append(new_temp)

	return iter(results)

def main():
	conf = SparkConf()
	conf.set("spark.default.parallelism", "32")
	#conf.set("spark.executor.memory", "1g")
	sc = SparkContext(appName="BatteryTemp", conf=conf)

	def tag_filter(ll):
		if ll.tag == 'KernelPrintk':
			return True
		elif ll.tag == 'Kernel-Trace':
			try:
				return ll.trace_event == 'thermal_temp'
			except:
				return False
		return False

	# Load LogLine tuples
	#all_logs = sc.textFile(processed_dir, use_unicode=False).map(lambda x: lib.logline_from_json(x))
	all_logs = sc.textFile(data_files, use_unicode=False).flatMap(ll_mapper)

	# Filter out any tags we don't care about
	#filtered = all_logs.filter(lambda line: line.tag in tags)
	filtered = all_logs.filter(tag_filter)

	# Group by (boot_id, date) so we have smaller chunks to work with
	keyed = filtered.keyBy(lambda ll: (ll.boot_id, ll.timestamp[:10], ll.line_num))

	partitioned = keyed.repartitionAndSortWithinPartitions(partitionFunc=lambda x: portable_hash(x[:2]), keyfunc=lambda x: (x[0],x[2]))
	intervals = partitioned.mapPartitions(process).map(lambda x: str(x))
	results = intervals.collect()

	fd = open("results", 'w')
	for res in results:
		s = '{}\n'.format(str(res))
		#print str(res)
		#fd.write(str(res) + '\n')
		fd.write(s)
	fd.close()

if __name__ == '__main__':
	now = time.time()
	main()
	print "Operation took {}s".format(time.time()-now)

