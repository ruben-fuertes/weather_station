#!/usr/bin/env python3

import pandas as pd
import mysql.connector
import sys
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


file = open(sys.argv[1])




def transformation(line):
	''' This function transform the data and returns a tupple
	with the values '''

	line = line.strip()
	time_str = line[0]
	press_str = float(line[1])
	temp1 = float(line[2])
	temp2 = float(line[3])
	hum = float(line[4])
	soil_hum = float(line[5])
	ticks = int(line[6])


for line in file:
	if check_quality(line):
		print(line)
