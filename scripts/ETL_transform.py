#!/usr/bin/env python3

import pandas as pd
import sys
import os
from datetime import datetime
from math import inf


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
	in a file to be read later by the ETL. Returns a DF with the averages/calculations'''

	# If l = 0 means that there's not files to process
	if l == 0:
		return 0

	df = pd.DataFrame([sub.split(",") for sub in l])
	d = dict()

	time = list(pd.to_datetime(df[0], format='%Y-%m-%dT%H:%M:%S'))
	time = time[len(time)//2]
	d["DATETIME"] = [time]
	press = round(pd.to_numeric(df[1]).mean(), 2)
	d["PRESS_Pa"] = [press]
	temp = round((pd.to_numeric(df[2]).mean() + pd.to_numeric(df[3]).mean()) / 2, 2)
	d["TEMP_C"] = [temp]
	hum = round(pd.to_numeric(df[4]).mean(), 2)
	d["HUMIDITY_PERC"] = [hum]
	soil_hum_raw = round(pd.to_numeric(df[5]).mean(), 2)
	d["HUMIDITY_SOIL_RAW"] = soil_hum_raw
	soil_hum = soil_hum_transform(pd.to_numeric(df[5]).mean())
	d["HUMIDITY_SOIL"] = [soil_hum]
	rain, last_tick = compute_rain(df[6], datetime.fromisoformat(df[0].iloc[-1]))
	if last_tick:
		with open('tmp/last_tick.txt', 'w') as lt:
			lt.write(str(df[0].iloc[-1]) + ',' + str(last_tick))
	d["RAIN_SINCE_LAST_READING_MM"] = [rain]

	out_df = pd.DataFrame.from_dict(data=d)

	return out_df


def compute(l):
	'''Takes a list of valid grouped data and returns a dataframe with
	the averages computed'''

	# If l = 0 means that there's not files to process
	if l == 0:
		return 0

	i = True
	for list in l:
		if i:
			avrg_df = compute_average(list)
			i = False
		avrg_df = pd.concat([avrg_df, compute_average(list)])

	avrg_df = avrg_df.reset_index(drop=True)
	return avrg_df


