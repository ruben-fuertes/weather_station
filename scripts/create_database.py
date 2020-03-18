#!/usr/bin/env python3

import pandas as pd
import mysql.connector
import re
from sys import argv

wd = argv[1] + "/scripts"

def create_database(db = "weather_station"):
	''' This function connects to the mariadb database and checks
	if the database exist, creating it in case it does not '''

	# Connects to mariadb database
	mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="ROOT_ACCESS_PASSWORD"
	)
	mycursor = mydb.cursor()

	# Creates the database in case it does not exist
	#mycursor.execute("DROP DATABASE IF EXISTS " + db) ##############

	mycursor.execute("SHOW DATABASES")

	databases = [re.compile("[a-zA-Z0-9_]+", ).findall(str(x))[0] for x in mycursor]

	if not db in databases:
		print("Creating database: " + db)
		mycursor.execute("CREATE DATABASE " + db)
	else:
		print("Database " + db + " already present")
		return
	mycursor.close()
	mydb.close()

	# Create the necessary tables
	mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="ROOT_ACCESS_PASSWORD",
	  database=db
	)

	mycursor = mydb.cursor()

	create_tables_query = open(wd + "/create_tables.sql").read()

	multi_sql(create_tables_query, mycursor)

	mycursor.execute("SHOW TABLES")

	for x in mycursor:
		print(x[0] + " successfully created")

	# Populate D_TIME dimension using populate_time.sql
	print("Populating time dimension...")
	populate_time_file = open(wd + "/populate_time.sql").read()
	multi_sql(populate_time_file, mycursor, sep = '---')
	print("Database created")

def multi_sql(query, cursor, sep = ';'):
	''' This function takes a string containing multiple SQL statements
	and executes it with the cursor '''

	for statement in query.split(sep):
		if len(statement.strip()) > 0:
			#print(statement)
			cursor.execute(statement)



create_database()
