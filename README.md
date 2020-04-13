# Weather station
This project is a full pipeline to collect data from sensors, sent to the raspberry pi through LoRa radiofrequency. It uses pyserial to read the data from the pi serial. It loads the data into a mariaDB database. The database is read by an API developed in Flask and it creates a webpage with the data. Individual components are conteinerized using docker.
