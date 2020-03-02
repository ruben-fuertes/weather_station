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
	seconds and returns a list broken by "sec" containing a list
	with the records for that time. sec must be smaller than the
	waiting time for the sender. Another list containing bad
	formatted lines is also returned.'''

	fl = []
	tl = []
	el = []
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
			el.append(line)
			pass
	if not appended:
		fl.append(tl)
	return fl, el

separated, error = separe_by_time(file)
for l in separated:
	p = [x.split(',')[0].split('T')[1] for x in l]
	#print(p)
#print(separated[0])


def soil_hum_transform(val):
        ''' This function transforms the raw value of the moisture sensor
        into a factor: "suelo seco", "suelo humedad baja", "suelo humedad media",
        "suelo humedad alta" that needs to be calibrated '''
        breakpoints = { 400 : "suelo seco",
                        800 : "suelo humedad baja",
                        1200 : "suelo humedad media",
                        inf : "suelo humedad alta"} # Arbitrary values needs to be calibrated

        for key in sorted(breakpoints):
                if val < key:
                        return breakpoints[key]


def compute_rain(col, curr_time, n=0.1):
	''' This function takes a column with the ticks of the rain gauge in
	a column of a DataFrame, the last number of ticks in the previous measure
	and the mm of rain in one tick and returns the ammount of rain in that lapse of time '''

	col = col.astype('int32')

	try:
		lt_file = open('tmp/last_tick.txt').read().split(',')
		last_time =  datetime.fromisoformat(lt_file[0])
		last_tick = int(lt_file[1])

		# Checks if the last stored tick was stored more than 3 hours ago
		if (curr_time - last_time).total_seconds()*60 < 180:
			prev_tick = int(last_tick)

		else:
			prev_tick = col.iloc[0]
	except:
		prev_tick = col.iloc[0]


	t0 = prev_tick
	total_rain = 0
	for i in col:
		delta = i-t0
		if abs(delta) > 1000:
			return None, None
		if delta > 0:
			total_rain += delta * n
		t0 = i
	last_tick = col.iloc[-1]
	total_rain = round(total_rain, 2)
	return total_rain, last_tick



def compute_average(l):
	''' This function takes a list with data and the previous last tick and returns the
	average/max for that time and the last nuber of rain ticks. Stores the last number of ticks
	in a file to be read later by the ETL'''

	df = pd.DataFrame([sub.split(",") for sub in l])

	time = list(pd.to_datetime(df[0], format='%Y-%m-%dT%H:%M:%S'))
	time = time[len(time)//2]
	press = pd.to_numeric(df[1]).mean()
	temp = (pd.to_numeric(df[2]).mean() + pd.to_numeric(df[3]).mean()) /2
	hum = pd.to_numeric(df[4]).mean()
	soil_hum = soil_hum_transform(pd.to_numeric(df[5]).mean())
	rain, last_tick = compute_rain(df[6], datetime.fromisoformat(df[0].iloc[-1]))
	if last_tick:
		with open('tmp/last_tick.txt', 'w') as lt:
			lt.write(str(df[0].iloc[-1]) + ',' + str(last_tick))

	return rain


print(compute_average(separated[0]))



#def compute_

#file = open(sys.argv[1])

#for line in file:
#	if check_quality(line):
#		print(line)
