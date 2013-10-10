#!/usr/bin/env python
"""parse.py: A script to parse AWS-MapReduce log files and generate reports"""
__author__ = "Keith Mascarenhas"
__email__ = "keithxm23@gmail.com"

import os
import datetime
from datetime import timedelta
from prettytable import PrettyTable # easy_install prettytable OR Look up https://code.google.com/p/prettytable/wiki/Installation
from operator import attrgetter
data = {}
for r,d,f in os.walk("./"): #Either point to directory containing syslog files or place this file in that directory
    for file in f:
        if file.endswith(".txt"):
			data[file] = {}
			with open(file) as f:
				seenJobComplete = False
				start = None
				end = None
				for line in f:
					if not seenJobComplete:
						seenJobComplete = 'Job complete' in line
						end = datetime.datetime.strptime(line.split(',')[0], "%Y-%m-%d %H:%M:%S")
						
					if seenJobComplete:
						if '=' in line:
							tmp = line.split('(main):')[1].split('=')
							data[file][tmp[0].strip()] = tmp[1].strip()
					else:
						if 'Running job:' in line:
							start = datetime.datetime.strptime(line.split(',')[0], "%Y-%m-%d %H:%M:%S")
							
					
				data[file]['Job Runtime'] =  str("%.2f" % ((end - start).seconds/60.0))


### Generating a simple report ###
pt = PrettyTable(["Program", "Job Running Time (mins)", "CPU Time Spent (ms)", "CPU Time Spent (mins)", "CIR", "COR", "MIR", "MOR", "RIR", "ROR"])
pt.align["Program"] = "l" # Left align program names
pt.padding_width = 1 # One space between column edges and contents (default)
for f in data:
	pt.add_row([f, data[f]['Job Runtime'], data[f]['CPU time spent (ms)'], str("%.2f" % (float(data[f]['CPU time spent (ms)'])/60000.0)), data[f]['Combine input records'], data[f]['Combine output records'], data[f]['Map input records'], data[f]['Map output records'], data[f]['Reduce input records'], data[f]['Reduce output records']])
print pt.get_string(sortby="Program")
print """Key:
CIR = Combine Input Records
COR = Combine Output Records
MIR = Map Input Records
MOR = Map Output Records
RIR = Reduce Input Records
ROR = Reduce Output Records"""