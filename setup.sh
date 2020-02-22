#!/bin/bash

WD=$(pwd)

GROUP=$(id -gn)

mkdir -p raw_data

echo "[Unit]
Description=Receiver Service
After=multi-user.target

[Service]
Type=simple
User=${USER}
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
