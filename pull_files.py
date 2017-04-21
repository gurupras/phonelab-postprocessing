import os,sys,argparse
import subprocess
import multiprocessing
from multiprocessing.pool import ThreadPool
import shlex
import datetime
import json
import signal

import logging
import pycommons
from pycommons import generic_logging
if __name__ == '__main__':
	generic_logging.init(level=logging.INFO)
logger = logging.getLogger(__file__)
#logging.getLogger('pycommons').setLevel(logging.ERROR)

# Load configuration
DIR=os.path.abspath(os.path.dirname(__file__))

# Hard-coded config path
CONFIG_PATH=os.path.join(DIR, '.config')
try:
	if not os.path.exists(CONFIG_PATH):
		raise IOError("Config file '.config' not found!")
	config = json.loads(open('.config', 'rb').read())
except IOError, e:
	# File does not exist
	logger.error(str(e))
	logger.info("Creating .config file. Please fill it out")
	with open(CONFIG_PATH, 'wb') as f:
		f.write(json.dumps({'user':""}, indent=2))
	sys.exit(-1)
except Exception, e:
	# Some other exception occured
	logger.error("Could not load config file:%s" % (str(e)))
	sys.exit(-1)

# Set up the backend with the specified config
BACKEND='%s@backend.phone-lab.org' % (config['user'])

BACKEND_PROCESSED_BASE_PATH='/mnt/data/logcat'
BACKEND_RAW_BASE_PATH='/mnt/data/upload'

def rsync_worker(remote_path, local_path, opts, dry, queue, **kwargs):
	rsync_cmdline = 'rsync %s %s %s' % (opts, remote_path, local_path)
	ret, stdout, stderr = _execv_worker(rsync_cmdline, dry, **kwargs)
	if queue:
		queue.put((remote_path, ret, stdout, stderr))
	return ret, stdout, stderr

def _execv_worker(cmdline, dry, **kwargs):
	try:
		signal.signal(signal.SIGINT, signal.SIG_IGN)
	except:
		pass
	return execv(cmdline, dry, **kwargs)

def execv(cmdline, dry=False, **kwargs):
	if not dry:
		return pycommons.run(cmdline, **kwargs)
	else:
		#logger.info("$>%s" % (cmdline))
		print cmdline
		return (0, '', '')

def setup_parser():
	parser = argparse.ArgumentParser()

	parser.add_argument('path', type=str, help='Out path')
	parser.add_argument('--date-range', action=pycommons.DateRangeAction, help='Range of dates', required=True)
	parser.add_argument('--dry', action='store_true', default=False, help='Dry run')

	device_group = parser.add_mutually_exclusive_group(required=True)
	device_group.add_argument('--device', '-d', action=pycommons.ListAction, help='Comma separated list of devices.')
	device_group.add_argument('--device-file', '-f', type=str, help='File containing list of devices')

	data_group = parser.add_mutually_exclusive_group(required=True)
	data_group.add_argument('--processed', '-p', action='store_true', default=False, help='Pull processed files')
	data_group.add_argument('--raw', '-r', action='store_true', default=False, help='Pull raw files')

	return parser

def get_remote_files(device, remote_path, local_path, dates, callback):
	try:
		regex = os.path.join(remote_path, '*.gz')
		ret, stdout, stderr = rsync_worker(regex, '', '', False, None)

		# remote_path is always: user@host:/..../.../...../time/year/month
		# and so...
		month = os.path.basename(remote_path)
		year = os.path.basename(os.path.dirname(remote_path))

		files = []
		allowed_files = ['%04d/%02d/%02d.out.gz' % (x.year, x.month, x.day) for x in dates]
		for line in stdout.split('\n'):
			line = line.strip()
			if not line:
				continue
			file = " ".join(line.split()).split()[4]
			ymd_file = os.path.join(year, month, file)
			if ymd_file not in allowed_files:
				continue
			file_size = int(" ".join(line.split()).split()[1].replace(",", ""))
			rpath = os.path.join(remote_path, file)
			lpath = os.path.join(local_path, file)
			callback(device, rpath, lpath, file_size)
	except Exception, e:
		logger.error('{}'.format(e))

def process_processed(path, devices, dates, dry):
	pool = multiprocessing.Pool(8)
	thread_pool = ThreadPool(8)

	manager = multiprocessing.Manager()
	queue = manager.Queue()

	total_size = [0]

	file_size_dict = {}
	failed = {}
	def update_file_size_dict(device, remote_path, local_path, size):
		file_size_dict[remote_path] = {
			'size': size,
			'remote': remote_path,
			'local': local_path
		}
		total_size[0] += size
		logger.debug("total_size = %d" % (total_size[0]))

	ym = set([(x.year, x.month) for x in dates])
	for d in devices:
		for year, month in ym:
			fpath = 'time/%04d/%02d' % (year, month)
			srcpath = os.path.join(BACKEND_PROCESSED_BASE_PATH, d, fpath)
			remote_path = '%s:%s' % (BACKEND, srcpath)

			outpath = os.path.join(path, d, fpath)
			# Make the outpath (if needed)
			if not os.path.exists(outpath):
				os.makedirs(outpath)

			thread_pool.apply_async(get_remote_files, args=(d, remote_path, outpath, dates, update_file_size_dict))
#			get_remote_files(d, remote_path, outpath, dates, update_file_size_dict)

	thread_pool.close()
	thread_pool.join()

	total_size = total_size[0]
	finished = 0

	logger.info("# files: %d" % (len(file_size_dict.keys())))
	for k, v in file_size_dict.iteritems():
		pool.apply_async(rsync_worker, args=(k, file_size_dict[k]['local'], '-avzpr', dry, queue))
		#rsync_worker(k, file_size_dict[k]['local'], '-avzpr', dry, queue)
	pool.close()
	try:
		i = 0
		while i < len(file_size_dict.keys()):
			path, ret, out, stder = queue.get()
			size = file_size_dict[path]['size']
			finished += size
			i += 1
#			pycommons.print_progress(finished, total_size)
#			logger.info("Finished: %d/%d" % (i, len(file_size_dict.keys())))

		pool.join()
	except KeyboardInterrupt:
		logger.warning("Terminating ...")
		return

	#print json.dumps(failed, indent=2)

def process_raw(path, devices, dates, dry):
	pool = multiprocessing.Pool()

	for d in devices:
		for date in dates:
			fpath = '%04d/%02d/%02d/' % (date.year, date.month, date.day)
			srcpath = os.path.join(BACKEND_RAW_BASE_PATH, d, fpath)

			outpath = os.path.join(path, d, fpath)
			outdir = os.path.dirname(outpath)
			# Make the outdir (if needed)
			if not os.path.exists(outdir):
				os.makedirs(outdir)

			rsync_cmdline = 'rsync -azupr %s:%s %s' % (BACKEND, srcpath, outpath)
			pool.apply_async(_execv_worker, args=(rsync_cmdline, dry))
	pool.close()
	pool.join()

def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	devices = []
	if args.device_file:
		with open(args.device_file) as f:
			for line in f:
				devices.append(line.strip())
	else:
		devices.extend(args.device)
	assert len(devices) >= 1, 'Need at least one device!'

	if args.processed:
		process_processed(args.path, devices, args.date_range, args.dry)
	elif args.raw:
		process_raw(args.path, devices, args.date_range, args.dry)


if __name__ == '__main__':
	main(sys.argv)

