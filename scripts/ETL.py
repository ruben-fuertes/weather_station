#!/usr/bin/env python3

import pandas as pd
import mysql.connector
import sys
from datetime import datetime
from math import inf

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


file = open(sys.argv[1])

def separe_by_time(file, sec = 100):
	''' This function takes a file and an interval of time in
	seconds and returns a list broken by "sec" a list with the
	records for that time. sec must be smaller than the waiting
	time for the sender '''

	fl = []
	tl = []
	lasttime = 0

	for line in file:
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
			pass
	if not appended:
		fl.append(tl)
	return fl

for l in separe_by_time(file):
	p = [x.split(',')[0].split('T')[1] for x in l]
	print(p)



def transformation(d):
	''' This function transform the data formatted as a csv
	and returns a tupple with the values '''

	d = d.strip()
	time_str = d[0]
	press = float(d[1])
	temp1 = float(d[2])
	temp2 = float(d[3])
	hum = float(d[4])
	soil_hum = float(d[5])
	rain_ticks = int(d[6])

	temp = (temp1 + temp2) / 2
	soil_hum_state = soil_hum_transform(soil_hum)


def soil_hum_transform(val):
	''' This function transforms the raw value of the moisture sensor
	into a factor: "suelo seco", "suelo humedad baja", "suelo humedad media",
	"suelo humedad alta" that needs to be calibrated '''
	breakpoints = {	400 : "suelo seco",
			800 : "suelo humedad baja",
			1200 : "suelo humedad media",
			inf : "suelo humedad alta"} # Arbitrary values needs to be calibrated

	for key in sorted(breakpoints):
		if val < key:
			return breakpoints[key]





#file = open(sys.argv[1])

#for line in file:
#	if check_quality(line):
#		print(line)
