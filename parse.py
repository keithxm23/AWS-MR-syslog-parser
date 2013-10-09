#!/usr/bin/env python
"""parse.py: A script to parse AWS-MapReduce log files and generate reports"""
__author__ = "Keith Mascarenhas"
__email__ = "keithxm23@gmail.com"

import os
data = {}
for r,d,f in os.walk("./"):
    for file in f:
        if file.endswith(".txt"):
			data[file] = {}
			with open(file) as f:
				seenJobComplete = False
				for line in f:
					if not seenJobComplete:
						seenJobComplete = 'Job complete' in line
					
					if seenJobComplete:
						if '=' in line:
							tmp = line.split('(main):')[1].split('=')
							data[file][tmp[0].strip()] = tmp[1].strip()
				

### Generating a simple report ###
# Syslog Filename | Cpu time spent (ms) | Cpu time spent (minutes) | Combine input records | Combine input records
print 'Syslog Name \t\t| CPU(ms) \t| CPU(mins) \t| Combine IP | Combine OP'
for f in data:
	print f+'\t|\t'+data[f]['CPU time spent (ms)']+'\t|\t'+str("%.2f" % (float(data[f]['CPU time spent (ms)'])/60000.0))+'\t|  '+data[f]['Combine input records']+' | '+data[f]['Combine output records']