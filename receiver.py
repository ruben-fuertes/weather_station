#!/usr/bin/env python3
import time
import serial
import os
import sys
from datetime import date, datetime


class ReadLine:
    def __init__(self, s):
        self.buf = bytearray()
        self.s = s

    def readline(self):
        i = self.buf.find(b"\n")
        if i >= 0:
            r = self.buf[:i+1]
            self.buf = self.buf[i+1:]
            return r
        while True:
            i = max(1, min(2048, self.s.in_waiting))
            data = self.s.read(i)
            i = data.find(b"\n")
            if i >= 0:
                r = self.buf + data[:i+1]
                self.buf[0:] = data[i+1:]
                return r
            else:
                self.buf.extend(data)


def readlineCR(port):
    '''Function used to join imput data from the Serial'''
    rv = b''
    while True:
        ch = port.read(1)
        rv += ch
        if ch==b"" or ch==b'\n':
            time.sleep(.5)
            return rv

def formatInput(input):
    '''This function gives format to the data received by
        the LoRa transciever and writes it into the output file'''
    s = ",".join(input.rstrip("+RCV=").split(",")[2:][:-2])
    print(s)
    # out_file = os.path.abspath(os.path.dirname(__file__)) + "/raw_data/" + date.today().strftime("%Y_%m_%d") + ".csv"
    # out_file = "/home/pi/receiver/" + "/raw_data/" + date.today().strftime("%Y_%m_%d") + ".csv"
    out_file = sys.argv[1] + "/raw_data/unprocessed/" + date.today().strftime("%Y_%m_%d") + ".csv"
    with open(out_file, "a+") as out:
        out.write(datetime.now().strftime("%Y-%m-%dT%H:%M:%S,") + s + '\n')



def LoRa_setup():
    ''' This function sets up of the LoRa transciever.
        It returns the open serial '''
    tries = 0
    while True:
        try:

            ser = serial.Serial('/dev/ttyS0',
                                 baudrate=115200,
                                 timeout=10
                                )
            time.sleep(1)
            ser.write("AT+RESET\r\n".encode('utf-8'))
            time.sleep(3)
            break
        except:
            tries += 1
            print("Retrying to set up for " + str(tries) + " time")
            continue
    return ser

def listen_serial(ser):
	''' Listen the serial port and waits for the data. It writes the raw
	received data and writes it into a file named as the current day.
	It takes the serial object as argument '''
	while True:
		if ser.in_waiting > 0:
			try:
				RL = ReadLine(ser)
				rcv = RL.readline()
				#rcv = readlineCR(ser).strip()
				formatInput(rcv.decode('utf-8'))
			except(UnicodeDecodeError):
				pass
				print("UnicodeDecodeError")
				print(rcv)
			except(serial.serialutil.SerialException):
				print("serial.serialutil.SerialException")
				pass
		else:
			time.sleep(2)

def main():
    ''' Main function '''
    serial = LoRa_setup()
    listen_serial(serial)

def test():
    i = 0
    while True:
        print(str(i) + '\n')
        i += 1
        time.sleep(1)

if __name__ == '__main__':
    main()
