import pandas as pd
import mysql.connector


def populate_live(df):
	'''This function takes a df with the data to insert in the LIVE table
	and loops through the values performing a ON DUPLICATE KEY UPDATE'''

	# Handles the possibility of no data to process
	if not type(df) is pd.core.frame.DataFrame:
		return 0

	mydb = mysql.connector.connect(
			host = "localhost",
			user = "root",
			passwd = "ROOT_ACCESS_PASSWORD",
			database = "weather_station"
	)
	mycursor = mydb.cursor()

	query_live = """INSERT INTO F_LIVE (
					HOUR_KEY_ID,
					DATETIME,
					HOUR,
					TIME_KEY_ID,
					TEMP_C,
					PRESS_Pa,
					HUMIDITY_PERC,
					HUMIDITY_SOIL_RAW,
					HUMIDITY_SOIL,
					RAIN_SINCE_LAST_READING_MM,
					UPDATED
					)
				VALUES	(
					%d,
					'%s',
					%d,
					%d,
					%.2f,
					%.2f,
					%.2f,
					%.2f,
					'%s',
					%d,
					%d)
	ON DUPLICATE KEY UPDATE		HOUR_KEY_ID = %d,
					DATETIME = '%s',
                                        HOUR = %d,
                                        TIME_KEY_ID = %d,
                                        TEMP_C = %.2f,
                                        PRESS_Pa = %.2f,
                                        HUMIDITY_PERC = %.2f,
                                        HUMIDITY_SOIL_RAW = %.2f,
                                        HUMIDITY_SOIL = '%s',
                                        RAIN_SINCE_LAST_READING_MM = %d,
					UPDATED = %d
		 """

	for i, row in df.iterrows():

		h = df.DATETIME.iloc[i].hour
		# l_k_id = int(df.DATETIME.iloc[i].strftime("%Y%m%d%H%M"))
		h_k_id = int(df.DATETIME.iloc[i].strftime("%Y%m%d%H"))
		t_k_id = int(df.DATETIME.iloc[i].strftime("%Y%m%d"))

		q_l = query_live % (
				# l_k_id,
				h_k_id,
				df.DATETIME.iloc[i],
				h,
				t_k_id,
				df.TEMP_C.iloc[i],
				df.PRESS_Pa.iloc[i],
				df.HUMIDITY_PERC.iloc[i],
				df.HUMIDITY_SOIL_RAW.iloc[i],
				df.HUMIDITY_SOIL.iloc[i],
				df.RAIN_SINCE_LAST_READING_MM.iloc[i],
				1,

				# l_k_id,
				h_k_id,
				df.DATETIME.iloc[i],
                                h,
                                t_k_id,
                                df.TEMP_C.iloc[i],
                                df.PRESS_Pa.iloc[i],
                                df.HUMIDITY_PERC.iloc[i],
                                df.HUMIDITY_SOIL_RAW.iloc[i],
                                df.HUMIDITY_SOIL.iloc[i],
                                df.RAIN_SINCE_LAST_READING_MM.iloc[i],
				1
				)
		mycursor.execute(q_l)
		mydb.commit()

	mycursor.close()
	mydb.close()

	return


def populate_hour():
	'''This function populates the F_HOUR table. It checks the rows just updated
	in F_LIVE (updated = 1) and groups by HOUR_KEY_ID, inserting or updating
	them into F_HOUR'''

	mydb = mysql.connector.connect(
			host = "localhost",
			user = "root",
			passwd = "ROOT_ACCESS_PASSWORD",
			database = "weather_station"
			)
	mycursor = mydb.cursor()

	query = """ INSERT INTO F_HOUR (
				HOUR_KEY_ID,
				DATETIME,
				HOUR,
				TIME_KEY_ID,
				TEMP_C,
				PRESS_Pa,
				HUMIDITY_PERC,
				HUMIDITY_SOIL_RAW,
				HUMIDITY_SOIL,
				RAIN_SINCE_LAST_READING_MM
				)

		SELECT
		  r_q.HOUR_KEY_ID,
		  r_q.DATETIME,
		  r_q.HOUR,
		  r_q.TIME_KEY_ID,
		  r_q.TEMP_C,
		  r_q.PRESS_Pa,
		  r_q.HUMIDITY_PERC,
		  r_q.HUMIDITY_SOIL_RAW,
		  r_q.HUMIDITY_SOIL,
		  r_q.RAIN_SINCE_LAST_READING_MM
		FROM(
		  SELECT
		    l.HOUR_KEY_ID,
		    CONVERT(DATE_FORMAT(l.DATETIME,'%Y-%m-%d-%H:00:00'),DATETIME) AS DATETIME,
		    l.HOUR,
		    l.TIME_KEY_ID,
		    AVG(l.TEMP_C) AS TEMP_C,
		    AVG(l.PRESS_Pa) AS PRESS_Pa,
		    AVG(l.HUMIDITY_PERC) AS HUMIDITY_PERC,
		    AVG(l.HUMIDITY_SOIL_RAW) AS HUMIDITY_SOIL_RAW,
		    (SELECT
		      live.HUMIDITY_SOIL
		    FROM
		      F_LIVE live
		    WHERE
		      live.HOUR_KEY_ID = l.HOUR_KEY_ID
		    GROUP BY
		      live.HUMIDITY_SOIL
		    ORDER BY
		      COUNT(*) DESC,
		      live.DATETIME DESC
		    LIMIT 1
		    ) HUMIDITY_SOIL,
		    SUM(l.RAIN_SINCE_LAST_READING_MM) AS RAIN_SINCE_LAST_READING_MM
		  FROM
		    F_LIVE l
		  WHERE
		    UPDATED = 1
		  GROUP BY
		    HOUR_KEY_ID
		) r_q -- real_query

	ON DUPLICATE KEY UPDATE
		HOUR_KEY_ID = r_q.HOUR_KEY_ID,
		DATETIME = r_q.DATETIME,
		HOUR = r_q.HOUR,
		TIME_KEY_ID = r_q.TIME_KEY_ID,
		TEMP_C = r_q.TEMP_C,
		PRESS_Pa = r_q.PRESS_Pa,
		HUMIDITY_PERC = r_q.HUMIDITY_PERC,
		HUMIDITY_SOIL_RAW = r_q.HUMIDITY_SOIL_RAW,
		HUMIDITY_SOIL = r_q.HUMIDITY_SOIL,
		RAIN_SINCE_LAST_READING_MM = r_q.RAIN_SINCE_LAST_READING_MM

	"""

	mycursor.execute(query)
	mydb.commit()
	mycursor.close()
	mydb.close()


def update_to_zero():
	''' This funciton takes the F_LIVE table and puts all the UPDATE column
	to 0'''

	mydb = mysql.connector.connect(
			host = "localhost",
			user = "root",
			passwd = "ROOT_ACCESS_PASSWORD",
			database = "weather_station"
			)
	mycursor = mydb.cursor()


	query = """ UPDATE F_LIVE
			SET
			  UPDATED = 0
		"""



	mycursor.execute(query)
	mydb.commit()

	mycursor.close()
	mydb.close()


def load(df):
	'''This function complies all the functions in ETL_load'''

	populate_live(df)

	populate_hour()

	update_to_zero()
