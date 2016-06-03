import sys
#import re2 as re
import re
import datetime
import ujson as json
from collections import namedtuple

# Individual prefix field patterns
re_uuid = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
re_time = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}'
re_logline = r'\d+'
re_tracetime = r'\[ *\d+\.\d+\]'
re_pid = r'\d+'
re_prio = r'[A-Z]'

# tracetime format is (boot_id timestamp line_num tracetime pid tid prio tag payload)
logline_regex_str = '^({}) ({}).* ({}) ({}) \s*({}) \s*({}) ({})(.*)$'.format(re_uuid, re_time, re_logline, re_tracetime, re_pid, re_pid, re_prio)
logline_regex = re.compile(logline_regex_str)

# Printk specific regex. The payload might be empty, in which case there is no space.
printk_regex_str = r'^.*({})(.*)$'.format(re_tracetime)
printk_regex = re.compile(printk_regex_str)

# Healthd specific regex
healthd_regex_str = r'^healthd: battery l=(\d+) v=(\d+) t=(-?\d+.\d+) h=(\d+) st=(\d+) c=(-?\d+) chg=(.*)$'
healthd_regex = re.compile(healthd_regex_str)

# Kernel-Trace specific regex
kernel_trace_regex_str = r'^(.*) \[(\d{3})\] .* (\d+\.\d+): (.*): (.*$)'
kernel_trace_regex = re.compile(kernel_trace_regex_str)

# Thermal temp trace
thermal_temp_regex_str = r'^sensor_id=(\d+) temp=(\d+)$'
thermal_temp_regex = re.compile(thermal_temp_regex_str)

# Fields for namedtuples
base_log_fields     = ['boot_id', 'timestamp', 'line_num', 'tracetime', 'pid', 'tid', 'prioi', 'tag']
printk_base_fields  = base_log_fields    + ['pk_type', 'pk_timestamp']
healthd_fields      = printk_base_fields + ['batt_level', 'batt_voltage', 'batt_temp', 'batt_health', 'batt_status', 'batt_current', 'batt_chg']
ktrace_base_fields  = base_log_fields    + ['trace_comm', 'trace_cpu', 'trace_ts', 'trace_event']
thermal_temp_fields = ktrace_base_fields + ['sensor_id', 'temp']

# Types of KernelPrintk lines that we recognize
PRINTK_UNKNOWN = 0
PRINTK_HEALTHD = 1
PRINTK_FREEZING = 2
PRINTK_FREEZE_ABORT = 3
PRINTK_RESTART_TASKS = 4

# Kernel traces that we recognize
KTRACE_UNKNOWN = 0
KTRACE_THERMAL_TEMP = 1

# Log Types, for serialization
LOG_TYPE_BASE = 0
LOG_TYPE_PRINTK = 1
LOG_TYPE_HEALTHD = 2
LOG_TYPE_KTRACE = 1000
LOG_TYPE_THERMAL = 1001

#####################   Exceptions   #####################

class UnknownLogTypeException(Exception):
	pass

class ParseException(Exception):
	def __init__(self, payload):
		Exception.__init__(self, 'Error parsing payload: ' + payload)

##########################################################

def get_logline_subclass(base, cls, getter):
	fields = getter(base.payload)
	if fields is None:
		return None
		#raise ParseException(base.payload)
	ll = cls(*(base[:len(base)-1] + fields))
	return ll

class LogLine(namedtuple('LogLine', base_log_fields + ['payload'])):
	__slots__ = ()
	def __str__(self):
		return json.dumps((LOG_TYPE_BASE,) + self)

	def convert_payload(self):
		'''Top-level logline conversion'''

		ll = cls = fields = getter = None

		if self.payload.strip() == '':
			# There have been some cases where the tag matches, but there isn't any
			# payload. We're not really interested in these lines.
			return self
		elif self.tag == 'Kernel-Trace':
			getter = get_ktrace_fields
			cls = KernelTraceLogLine
		elif self.tag == 'KernelPrintk':
			getter = get_printk_fields
			cls = PrintkLogLine
		else:
			return self

		ll = get_logline_subclass(self, cls, getter)
		if ll is None:
			# Garbage line, log and ignore
			print "Error parsing line: " + str(self)
			return self

		return ll.convert_payload()

class HealthdLogLine(namedtuple('HealthdLogLine', healthd_fields)):
	__slots__ = ()
	def __str__(self):
		return json.dumps((LOG_TYPE_HEALTHD,) + self)

	def convert_payload(self):
		return self

class PrintkLogLine(namedtuple('PrintkLogLine', printk_base_fields + ['payload'])):
	__slots__ = ()
	def __str__(self):
		return json.dumps((LOG_TYPE_PRINTK,) + self)

	def convert_payload(self):
		if self.pk_type == PRINTK_HEALTHD:
			return get_logline_subclass(self, HealthdLogLine, get_healthd_fields)
		else:
			return self

class KernelTraceLogLine(namedtuple('KernelTraceLogLine', ktrace_base_fields + ['payload'])):
	__slots__ = ()
	def __str__(self):
		return json.dumps((LOG_TYPE_KTRACE,) + self)

	def convert_payload(self):
		if self.trace_event == 'thermal_temp':
			return get_logline_subclass(self, ThermalTempLogLine, get_thermal_temp_fields)
		else:
			return self

class ThermalTempLogLine(namedtuple('ThermalTempLogLine', thermal_temp_fields)):
	__slots__ = ()
	def __str__(self):
		return json.dumps((LOG_TYPE_THERMAL,) + self)

	def convert_payload(self):
		return self

def logline_from_json(line):
	data = json.loads(line)
	tp = data[0]
	data = data[1:]

	if tp == LOG_TYPE_BASE:
		return LogLine(*data)
	elif tp == LOG_TYPE_PRINTK:
		return PrintkLogLine(*data)
	elif tp == LOG_TYPE_HEALTHD:
		return HealthdLogLine(*data)
	elif tp == LOG_TYPE_KTRACE:
		return KernelTraceLogLine(*data)
	elif tp == LOG_TYPE_THERMAL:
		return ThermalTempLogLine(*data)
	else:
		raise UnknownLogTypeException()

def logline_from_text(line):
	# The prefix is either 128 or 256 bytes (max), depending on when the device
	# was updated. The only affected part is the tag and payload.

	m = logline_regex.match(line.strip())
	if m is None:
		return None

	boot_id = m.group(1)
	timestamp = m.group(2)
	line_num = int(m.group(3))
	tracetime = float(m.group(4)[1:len(m.group(4))-1])
	pid = int(m.group(5))
	tid = int(m.group(6))
	prio = m.group(7)

	# Not to separate the tag and payload. This is overly complicated because
	# we broke locat when we introduced the boot_id. Unfortunately, the header
	# had a fixed size buffer of 128 bytes which cause some tags to be truncated.
	# This attempts to get the tag, or at leat the truncated portion of it.

	start = m.end(7)+1
	pos = start + 8		# tags have min length of 8 (logcat)
	tag = None

	while pos < 255 and pos < len(line)-1 and line[pos] != '{' and line[pos] != ' ':
		if line[pos] == ':' and line[pos+1] == ' ':
			tag = line[start:pos]
			break
		pos += 1

	if tag is None:
		# We're on the colon
		tag = line[start:127]
		payload = line[127:]
	else:
		payload = line[pos+2:]

	tag = tag.strip()

	return LogLine(boot_id, timestamp, line_num, tracetime, pid, tid, prio, tag, payload)


#################   LogLine --> Tag-Specific Class Converters   ################# 

def get_healthd_fields(payload):
	m = healthd_regex.match(payload)
	if m is None:
		return None

	level = int(m.group(1))
	voltage = int(m.group(2))
	temp = float(m.group(3))
	health = int(m.group(4))
	status = int(m.group(5))
	current = int(m.group(6))
	charging = m.group(7)

	return (level, voltage, temp, health, status, current, charging)

def get_printk_fields(payload):
	# Base printk fields
	m = printk_regex.match(payload.strip())
	if m is None:
		return None

	ts = float(m.group(1)[1:len(m.group(1))-1])
	pk_payload = m.group(2).strip()
	tp = PRINTK_UNKNOWN

	if 'healthd: battery' in payload:
		tp = PRINTK_HEALTHD
	elif 'Freezing user space processes' in payload:
		tp = PRINTK_FREEZING
	elif 'Freezing of tasks aborted' in payload:
		tp = PRINTK_FREEZE_ABORT
	elif 'Restarting tasks ... done.' in payload:
		tp = PRINTK_RESTART_TASKS

	return (tp, ts, pk_payload)

def get_ktrace_fields(payload):
	m = kernel_trace_regex.match(payload.strip())
	if m is None:
		return None

	comm = m.group(1).strip()
	cpu = int(m.group(2))
	ts = float(m.group(3))
	event = m.group(4)
	trace_payload = m.group(5).strip()

	return (comm, cpu, ts, event, trace_payload)

def get_thermal_temp_fields(payload):
	m = thermal_temp_regex.match(payload.strip())
	if m is None:
		return None

	return (int(m.group(1)), int(m.group(2)))

#################################################################################

class BootSession:
	boot_id = ""
	count = 1
	low_line = 0
	high_line = 0

	def __init__(self, logline=None):
		if logline is not None:
			self.boot_id = logline.boot_id
			self.low_line = logline.line_num
			self.high_line = self.low_line
			self.count = 1

# Merge two boot session metadata objects
def merge_session_data(data1, data2):
	session = BootSession()
	session.boot_id = data1.boot_id
	session.count = data1.count + data2.count

	if data1.low_line < data2.low_line:
		session.low_line = data1.low_line
	else:
		session.low_line = data2.low_line

	if data1.high_line > data2.high_line:
		session.high_line = data1.high_line
	else:
		session.high_line = data2.high_line

	return session


#################################################################################

class BatteryInterval(namedtuple('BatteryInterval', 'level, temps, boot_id, start_line, end_line, start_time, end_time')):
	__slots__ = ()

	def __str__(self):
		return json.dumps(self)

	def __repr__(self):
		return json.dumps(self)

