#!/usr/bin/env python3

import ETL_extract as extr
import ETL_transform as tran
import ETL_load as load
from sys import argv
import time
from datetime import datetime


def run_ETL(min, wd):
	'''This function activates the ETL each "min" minutes '''

	t = min * 60

	while True:

		try:
			df = tran.compute(extr.file_processer(wd), wd)
			load.load(df)
		except:
			pass
		time.sleep(t)

# wd = argv[1]

wd = "/home/pi/weather_station/scripts/raw_data/"

run_ETL(2, wd)
