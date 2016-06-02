import os,sys,argparse,re
import json
import itertools

import matplotlib.pyplot as plt
from pylab import rc

import crop

import logging
from pycommons import generic_logging
if __name__ == '__main__':
	generic_logging.init(level=logging.DEBUG)
logger = logging.getLogger(__file__)

import numpy as np
from collections import Counter

rc('font',**{'family':'serif','serif':['Times'], 'size': 9})
rc('text', usetex=True)
rc('legend', fontsize=6, labelspacing=0.2)

colors = {0 : 'r', 1 : 'b', 2 : 'g', 3 : 'k'}

rc('font',**{'family':'serif','serif':['Times'], 'size': 9})
rc('text', usetex=True)
#rc('legend', fontsize=6, labelspacing=0.2)
rc('legend', fontsize=7.5, labelspacing=0.2)
rc('xtick', labelsize=8)
rc('ytick', labelsize=8)

rasterized=False

def new_fig(width=3.3, height=2.5, size=None):
	if size is 'half':
		return plt.figure(figsize=(width/2, height/2))
	elif size is 'third':
		return plt.figure(figsize=(width/3, height/3))
	elif size is 'fourth':
		return plt.figure(figsize=(width/4, height/4))
	else:
		return plt.figure(figsize=(width, height))

def save_plot(outdir, name, fig=None, **kwargs):
	if outdir:
		path = os.path.join(outdir, name)
	else:
		path = name
	logger.info("path: %s" % (path))
	if fig:
		fig.savefig(path, bbox_inches='tight', rasterized=rasterized, **kwargs)
		plt.close(fig)
	else:
		plt.savefig(path, bbox_inches='tight', rasterized=rasterized, **kwargs)
		plt.close()
	crop.main(['crop.py', path])


def plot_cdf(points, ax, labels, xlabel, ylabel='\\textbf{CDF}', binwidth=0.01,
		title=None, color='k', legend=True, markers=False):
	if not isinstance(points, list):
		points = [points]
	if not labels:
		labels = [None for x in range(len(points))]
	if not color:
		color= [None for x in range(len(points))]
	assert len(points) == len(labels)

	_markers = ['o', 's', 'v', 'h', 'p', '8', 'D', 'x', '+', '*', 'd', '^']
	if not markers:
		markers = [None for x in points]
	else:
		markers = _markers
	assert len(markers) >= len(points), 'Not enough markers'

	for idx, pts, label, color in itertools.izip(xrange(len(points)), points, labels, color):
		try:
			pts = sorted(pts)
			pts = round_list(pts)

			counts_dict = Counter(pts)
			unique_pts = sorted(set(pts))
			counts = [counts_dict[x] for x in unique_pts]
			cumsum = np.cumsum(counts)
			cumsum = [x / float(sum(counts)) for x in cumsum]
			x_axis = unique_pts
			# From 0
			cumsum.insert(0, 0)
			x_axis.insert(0, x_axis[0])
			logger.debug('len(cumsum): %d, len(xaxis): %d' % (len(cumsum), len(x_axis)))
			if not color:
				ax.plot(x_axis, cumsum, label=label, marker=markers[idx], markersize=3, markeredgewidth=0, rasterized=rasterized)
			else:
				ax.plot(x_axis, cumsum, label=label, marker=markers[idx], markersize=3, markeredgewidth=0, rasterized=rasterized, color=color)
		except Exception, e:
			print e
			continue

#	xmin = min(x_axis)
#	xmax = max(x_axis)
#	ax.set_xlim((min(x_axis), max(x_axis)
	ax.set_yticks(np.arange(0, 1.1, 0.2))

	ax.set_xlabel(xlabel)
	#XXX: We don't show CDF on every plot ...
	ax.set_ylabel(ylabel)

	if title:
		ax.set_title(title)

	if legend:
		ax.legend(loc='best', numpoints=1, markerscale=1.5)

def boxplot(ax, values, positions, width, color, **kwargs):
	overall_p85 = None
	for v, p in itertools.izip(values, positions):
		if len(v) == 0:
			continue
		p25 = np.percentile(v, 25)
		p50 = np.percentile(v, 50)
		p75 = np.percentile(v, 75)
		ax.bar(p, p75-p25, bottom=p25, width=width, color=color, edgecolor='none', **kwargs)
		ax.plot([p, p+width], [p50, p50], color='w', linewidth=0.5)

def plot_temp_battery_correlation(ax, temp_levelduration_dict, temp_busyness_dict=None, d_means=None, d_medians=None):
	sorted_temps = sorted(temp_levelduration_dict.keys())

	if temp_busyness_dict is not None:
		ax1 = ax.twinx()
		busy_means = []
		busy_medians = []

	for t in sorted_temps:
		for d in temp_levelduration_dict[t]:
#		   d = np.mean(temp_levelduration_dict[t])
			ax.scatter(t, d, linewidths=0, edgecolor='none')
		if temp_busyness_dict:
			busy_means.append(np.mean(temp_busyness_dict[t]))
			busy_medians.append(np.median(temp_busyness_dict[t]))
	if d_means:
		ax.plot(sorted_temps, d_means, 'r', label='Mean Duration')
	if d_medians:
		ax.plot(sorted_temps, d_medians, 'g', label='Median Duration')

	# Now set up the figure
	ax.set_xlabel('\\textbf{Temperature} $^\circ$C')
	ax.set_yscale('log')
	ax.set_ylabel('\\textbf{Time Spent at Battery Level} s')

	if temp_busyness_dict is not None:
		ax1.plot(sorted_temps, busy_means, 'g', label='Mean Busyness')
		ax1.set_ylim([0, 100])
		ax1.set_ylabel('\\textbf{CPU Busy} \\%')

		h1, l1 = ax.get_legend_handles_labels()
		h2, l2 = ax1.get_legend_handles_labels()

		ax1.legend(h1 + h2, l1 + l2, bbox_to_anchor=(0., 1.02, 1., .102), borderaxespad=0., loc=3, ncol=2, mode='expand', markerscale=0.8)


def round_list(values):
	return [_round(v) for v in values]

def _round(v):
	round_dict = {0.0001 : 3, 0.001 : 2, 0.01 : 1, 0.1 : 1, 1 : 1, 10 : 0, 100 : -1, 1000 : -2}
	round_list = sorted(round_dict.keys(), reverse=True)
	for r in round_list:
		if abs(v) > r:
			ret = round(v, round_dict[r])
			if ret == 0:
				ret = round(v, round_dict[r] + 1)
			return ret
	return v

colors= ['r', '#751D3C', 'b', 'g', 'm', 'k', 'c', '#BFACDA', '#E96048', '#E88BDD', '#AD68FC', '#DAE480', '#A868FC', 'y']
devices = [
	'1a28ea49f4206010fee054f9bdb86f822dc4dd28',
	'1b0676e5fb2d7ab82a2b76887c53e94cf0410826',
	'2747d54967a32fc95945671b930d57c1d5a9ac02',
	'4affacf02c96acd83e34d8085159150b753105bb',
	'52a71d8a0140f8b994fa1b69492dd41a7cfc4b4f',
	'545e7aba064dd4e65e8c22caa909489657898afe',
	'8860ffed77873823ef1e971db16631ea4d26d635',
	'9714ee0ce5a5d75710902d79e0dd34e683d1ae76',
	'bca9d14152d010ab203dffd36d75b0da84ac4767',
	'ed05b5fafc2aa4aa7a0f2527f918fb1f3272ce8a',
	'8d0376587d6091bed78e081b614ca485fb098c23',
	'b054b98ec2724dd02d0b5f97edce95a9cb713352',
]
device_colors = {}
for d, c in itertools.izip(devices, colors):
	device_colors[d] = c


