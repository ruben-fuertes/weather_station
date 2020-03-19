#!/bin/bash


# This function creates a service file that makes the file receiver.py run as a daemon
create_service_receiver(){

	echo "Creating receiver service..."

	WD=$(pwd)

	GROUP=$(id -gn)

	if [ ! -f /lib/systemd/system/receiver.service ] || [ $(grep -x -c "WorkingDirectory=$WD" /lib/systemd/system/receiver.service) -eq 0 ]
	then

	echo "[Unit]
Description=Receiver Service
After=multi-user.target

[Service]
Type=simple
User="$USER"
Group="$GROUP"
ExecStart=/usr/bin/python3 "$WD"/receiver.py "$WD"
WorkingDirectory="$WD"
StandardOutput=syslog
StandardError=syslog

[Install]
WantedBy=multi-user.target" > receiver.service

	sudo cp receiver.service /lib/systemd/system/receiver.service

	sudo rm receiver.service

	sudo systemctl daemon-reload

	sudo systemctl enable receiver.service
	sudo systemctl start receiver.service

	echo "Receiver service ready"

	else

	echo "Receiver service already running"

	fi
}



# This function creates a service file that makes ETL_marter.py run as a daemon
create_service_ETL(){

	echo "Creating ETL service..."

        WD=$(pwd)

        GROUP=$(id -gn)

        if [ ! -f /lib/systemd/system/ETL.service ] || [ $(grep -x -c "WorkingDirectory=$WD" /lib/systemd/system/ETL.service) -eq 0 ] || [ 1 -eq 1 ]
        then

        echo "[Unit]
Description=ETL service
After=multi-user.target

[Service]
Type=simple
User="$USER"
Group="$GROUP"
ExecStart=/usr/bin/python3 "$WD"/scripts/ETL_master.py "$WD/raw_data"
WorkingDirectory="$WD"
StandardOutput=syslog
StandardError=syslog

[Install]
WantedBy=multi-user.target" > ETL.service

        sudo cp ETL.service /lib/systemd/system/ETL.service

        sudo rm ETL.service

        sudo systemctl daemon-reload

        sudo systemctl enable ETL.service
        sudo systemctl start ETL.service

        echo "ETL service ready"

        else

        echo "ETL service already running"

	fi
}

# this function creates the volumes, services and backup directories
docker_create_dirs() {
	[ -d ./services ] || mkdir ./services
	[ -d ./volumes ] || mkdir ./volumes
	[ -d ./backups ] || mkdir ./backups

}


#function copies the template yml file to the local service folder and appends to the docker-compose.yml file
yml_builder() {

	service="services/$1/service.yml"

	mkdir -p ./services/$1
	echo "...pulled full $1 from template"
	rsync -a -q .templates/$1/ services/$1/


	#if an env file exists check for timezone
	[ -f "./services/$1/$1.env" ] && timezones ./services/$1/$1.env

	#add new line then append service
	echo "" >>docker-compose.yml
		cat $service >>docker-compose.yml

	#make sure terminal.sh is executable
	[ -f ./services/$1/terminal.sh ] && chmod +x ./services/$1/terminal.sh

}



# Creates the docker-compose.yml

yml_writter() {

	docker_create_dirs

        #Cointainers that will be installed
        declare -a containers=("mariadb")

        touch docker-compose.yml
        echo "version: '2'" >docker-compose.yml
        echo "services:" >>docker-compose.yml

        #first run service directory wont exist
        [ -d ./services ] || mkdir services

        #Run yml_builder of all selected containers
        for container in "${containers[@]}"; do
                echo "Adding $container container"
                yml_builder "$container"
        done

	echo "running 'docker-compose up -d'"

	docker-compose up -d

}


# This function check timezone and update env file
timezones() {

	env_file=$1
	TZ=$(cat /etc/timezone)

	#test for TZ=
	[ $(grep -c "TZ=" $env_file) -ne 0 ] && sed -i "/TZ=/c\TZ=$TZ" $env_file

}


# Checks if there is updates for the project
update_project() {

	echo "checking for project update"
	git fetch origin master

	if [ $(git status | grep -c "Your branch is up to date") -eq 1 ]; then
		#delete .outofdate if it exisist
		[ -f .outofdate ] && rm .outofdate
		echo "Project is up to date"

	else
		echo "An update is available for the project"
		if [ ! -f .outofdate ]; then
			whiptail --title "Project update" --msgbox "An update is available for the project\nYou will not be reminded again until you next update" 8 78
			touch .outofdate
		fi
	fi


}


command_exists() {
	command -v "$@" >/dev/null 2>&1
}


# Installs docker if it does not exist
install_docker() {

	inst=0

	if command_exists docker; then
		echo "docker already installed"
	else
		echo "Install Docker"
		curl -fsSL https://get.docker.com | sh
		sudo usermod -aG docker $USER
		inst=1
	fi

	if command_exists docker-compose; then
		echo "docker-compose already installed"
	else
		echo "Install docker-compose"
		sudo apt install -y docker-compose
		inst=1
	fi

	if [ $inst -eq 1 ]; then
		if (whiptail --title "Restart Required" --yesno "It is recommended that you restart you device now. Select yes to do so now" 20 78); then
			sudo reboot
		fi
	fi

}


# Checks the architecture and exits if not suported

check_architecture() {

	sys_arch=$(uname -m)

	if [ $(echo "$sys_arch" | grep -c "arm") ]; then
		echo "Correct architecture"
		echo "Starting setup..."

	else
		echo "Architecture not supported"
		exit
	fi

}


# This function checks if necessary packages are installed and installs the ones that are not.

install_python_requirements() {

	# Hash table containing package name and import
	declare -A packages=( ["pandas"]="pandas" ["numpy"]="numpy" ["mysql-connector"]="mysql" )

	echo "Checking python requirements"

	for pack in "${!packages[@]}"; do

		if [ $(python3 -c "import ${packages[$pack]}" > /dev/null 2>&1; echo $?) -eq 1 ]
		then

			echo "installing $pack"
			pip3 install $pack

		else

			echo "Package $pack already installed"

		fi
	done

}


# This function executes the script that create the database
# into docker containing MariaDB

create_database() {

	WD=$(pwd)

	python3 "$WD"/scripts/create_database.py "$WD"

}

main() {

	check_architecture

	#update_project

	install_python_requirements

	mkdir -p raw_data raw_data/processed raw_data/unprocessed raw_data/backup raw_data/processed/valid raw_data/processed/not_valid raw_data/tmp

	create_service_receiver

	install_docker

	yml_writter

	create_database

	create_service_ETL
}

main

#install_python_requirements
