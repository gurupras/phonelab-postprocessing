#import ujson as json
import json
from lib import BatteryInterval
import numpy as np
import matplotlib.pyplot as plt
import plot

def main():
	fd = open('results', 'r')
	data = {}

	for line in fd:
		d = json.loads(line)
		interval = BatteryInterval(*d)
		duration = interval.end_time - interval.start_time
		try:
			temp = int(np.mean(interval.temps))
		except:
			print line
			continue

		if not temp in data:
			data[temp] = []
		data[temp].append(duration)
	#print data
	fd.close()

	ax = plot.new_fig().add_subplot(111)
	means = [np.mean(data[x]) for x in sorted(data.keys())]
	plot.plot_temp_battery_correlation(ax, data, d_means=means)
	plt.show()

if __name__ == '__main__':
	main()
