import os,sys,argparse
import subprocess
import shlex
import datetime
import json

import logging
import pycommons
from pycommons import generic_logging
if __name__ == '__main__':
	generic_logging.init(level=logging.DEBUG)
logger = logging.getLogger(__file__)

# Load configuration
DIR=os.path.abspath(os.path.dirname(__file__))

CONFIG_PATH=os.path.join(DIR, '.config')
try:
	if not os.path.exists(CONFIG_PATH):
		raise IOError("Config file '.config' not found!")
	config = json.loads(open('.config', 'rb').read())
except IOError, e:
	logger.error(str(e))
	logger.info("Creating .config file. Please fill it out")
	with open(CONFIG_PATH, 'wb') as f:
		f.write(json.dumps({'user':""}, indent=2))
	sys.exit(-1)
except Exception, e:
	logger.error("Could not load config file:%s" % (str(e)))
	sys.exit(-1)

BACKEND='%s@backend.phone-lab.org' % (config['user'])

BACKEND_PROCESSED_BASE_PATH='/mnt/data/logcat'
BACKEND_RAW_BASE_PATH='/mnt/data/upload'

def execv(cmdline, dry=False):
	if not dry:
		pycommons.run(cmdline)
	else:
		logger.info("$>%s" % (cmdline))

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

def process_processed(path, devices, dates, dry):
	for d in devices:
		rsync_paths = []
		for date in dates:
			fpath = 'time/%04d/%02d/%02d.out.gz' % (date.year, date.month, date.day)
			srcpath = os.path.join(BACKEND_PROCESSED_BASE_PATH, d, fpath)

			outpath = os.path.join(path, d, fpath)
			outdir = os.path.dirname(outpath)
			# Make the outdir (if needed)
			if not os.path.exists(outdir):
				os.makedirs(outdir)

			rsync_paths.append('%s:%s' % (BACKEND, srcpath))
		rsync_cmdline = 'rsync -avzupr --ignore-missing-args ' + ' '.join(rsync_paths) + ' ' + outdir
		try:
			execv(rsync_cmdline, dry)
		except:
			logger.warning("Could not fetch %s" % (d))

def process_raw(path, devices, dates, dry):
	for d in devices:
		rsync_paths = []
		for date in dates:
			fpath = 'time/%04d/%02d/%02d/' % (date.year, date.month, date.day)
			srcpath = os.path.join(BACKEND_RAW_BASE_PATH, d, fpath)

			outpath = os.path.join(path, d, fpath)
			outdir = os.path.dirname(outpath)
			# Make the outdir (if needed)
			if not os.path.exists(outdir):
				os.makedirs(outdir)

			rsync_paths.append('%s:%s' % (BACKEND, srcpath))
		rsync_cmdline = 'rsync -avzupr --ignore-missing-args ' + ' '.join(rsync_paths) + ' ' + outdir
		try:
			execv(rsync_cmdline, dry)
		except:
			logger.warning("Could not fetch %s" % (d))


def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	devices = []
	if args.device_file:
		with open(args.device_file) as f:
			for line in f:
				devices.append(line.strip())
	else:
		devices.append(args.device)
	assert len(devices) >= 1, 'Need at least one device!'

	if args.processed:
		process_processed(args.path, devices, args.date_range, args.dry)
	elif args.raw:
		process_raw(args.path, devices, args.date_range, args.dry)


if __name__ == '__main__':
	main(sys.argv)

