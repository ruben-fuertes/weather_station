CREATE TABLE F_LIVE (
	LIVE_KEY_ID 	int NOT NULL AUTO_INCREMENT,
	DATETIME 	DATETIME NOT NULL UNIQUE,
	HOUR		int NOT NULL,
	TIME_KEY_ID 	int NOT NULL,
	TEMP_C 		numeric,
	PRESS_Pa 	numeric,
	HUMIDITY_PERC 	numeric,
	HUMIDITY_SOIL 	varchar(255),
	RAIN_SINCE_LAST_READING_MM numeric,
	PRIMARY KEY (LIVE_KEY_ID)
);

CREATE TABLE D_TIME (
	TIME_KEY_ID 	int NOT NULL AUTO_INCREMENT,
	DATE 		date NOT NULL UNIQUE,
	YEAR 		int NOT NULL,
	MONTH 		int NOT NULL,
	MONTH_NAME	varchar(12) NOT NULL,
	MONTH_NOMBRE	varchar(12) NOT NULL,
	DAY		int NOT NULL,
	DAY_NAME	varchar(12) NOT NULL,
	DAY_NOMBRE	varchar(12) NOT NULL,
	WEEK	 	int NOT NULL,
	WEEKEND_FLAG	int DEFAULT 0 CHECK (WEEKEND_FLAG in (0,1)),
	PRIMARY KEY (TIME_KEY_ID)
);

CREATE TABLE F_HOUR (
	HOUR_KEY_ID 	int NOT NULL AUTO_INCREMENT,
	DATETIME 	DATETIME NOT NULL UNIQUE,
	HOUR		int NOT NULL,
	TIME_KEY_ID 	int NOT NULL,
	TEMP_C 		numeric,
	PRESS_Pa 	numeric,
	HUMIDITY_PERC 	numeric,
	HUMIDITY_SOIL 	varchar(255),
	RAIN_SINCE_LAST_READING_MM numeric,
	PRIMARY KEY (HOUR_KEY_ID)
);

ALTER TABLE F_LIVE ADD CONSTRAINT F_LIVE_fk0 FOREIGN KEY (TIME_KEY_ID) REFERENCES D_TIME(TIME_KEY_ID);

ALTER TABLE F_HOUR ADD CONSTRAINT F_HOUR_fk0 FOREIGN KEY (TIME_KEY_ID) REFERENCES D_TIME(TIME_KEY_ID);

