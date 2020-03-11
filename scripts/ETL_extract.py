#!/usr/bin/env python3

import pandas as pd
import sys
import os
from datetime import datetime


def check_quality(line):
	''' This function takes a line of the raw data and checks if it
	the values are possible and are well formated '''

	line = line.split(",")
	try:
		time_str = line[0]
		press_str = float(line[1])
		temp1 = float(line[2])
		temp2 = float(line[3])
		hum = float(line[4])
		soil_hum = float(line[5])
		ticks = int(line[6])

	except:
		return False

	if (press_str < 500 or press_str > 2000
		or temp1 < -30 or temp1 > 55
		or temp2 < -30 or temp2 > 55
		or hum < 0 or hum > 100
		or soil_hum < 0 or soil_hum > 10000
		or ticks < 0):

		return False

	# Check if the time is well formated
	try:
		time = datetime.fromisoformat(time_str)
	except:
		return False

	return True


def separe_by_time(readfile, sec = 100):
	''' This function takes a file and an interval of time in
	seconds and returns a list broken by "sec" containing a list
	with the records for that time. sec must be smaller than the
	waiting time for the sender. Another list containing bad
	formatted lines is also returned.'''

	fl = []
	tl = []
	el = []
	appended = True
	lasttime = 0

	for line in readfile.split('\n'):
		line = line.strip()
		if check_quality(line):
			time = line.strip().split(',')[0]
			time = datetime.fromisoformat(time)
			if lasttime == 0 or (time - lasttime).seconds < sec:
				appended = False
				tl.append(line)
			else:
				appended = True
				fl.append(tl)
				tl = []
			lasttime = time
		else:
			el.append(line)
			pass
	if not appended:
		fl.append(tl)
	return fl, el

def file_processer(b_d):
	'''This function takes the base directory and takes the oldest file in the unprocessed
	directory and handles the data. It returns the valid data in a list of lists
	with the data grouped by time.'''

	up = b_d + "unprocessed/"
	pr_v = b_d + "processed/valid/"
	bk = b_d + "backup/"
	pr_nv = b_d + "processed/not_valid/"

	uplist = sorted(os.listdir(up))

	# Returns 0 if there is no files in unprocessed
	if not len(uplist):
		return 0

	filename = uplist[0]
	filepath = up + filename
	with open(filepath) as f:
		readfile = f.read()

	with open(bk + filename, 'a+') as f:
		f.write(readfile)

	os.remove(filepath)

	# Adds the previous last group of data stored in tmp
	if filename in os.listdir("tmp/"):
		readfile = open("tmp/" + filename).read() + readfile
		os.remove("tmp/" + filename)

	val_l, noval_l = separe_by_time(readfile, sec=100)


	if len(val_l):
		with open(pr_v + filename, 'a+') as f:
			for l in val_l:
				for i in l:
					f.write(i + '\n')

		# Write the last group in tmp if processing current day
		lastdate = datetime.fromisoformat(val_l[-1][-1].split(',')[0]).date()
		today = datetime.now().date()
		if lastdate == today:
			with open("tmp/" + filename, 'w') as f:
				for l in val_l.pop(-1):
					f.write(l)

	if len(noval_l):
		with open(pr_nv + filename, 'a+') as f:
			for l in noval_l:
				f.write(l + '\n')


	return val_l
