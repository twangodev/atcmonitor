import subprocess
from enum import IntEnum
import json
from datetime import datetime
from os import environ
from kafka import KafkaProducer
import pandas as pd 
import csv


TESTMODE = 1


hostname = "192.168.128.229"
port = 30003
bootstrap_servers = environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
PRODUCER_TOPIC = "adsb.live" 
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

roles = {
    "MSG1": [1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0],
    "MSG2": [1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,0,0,0,0,0,1],
    "MSG3": [1,1,1,1,1,1,1,1,1,1,0,1,0,0,1,1,0,0,1,1,1,1],
    "MSG4": [1,1,1,1,1,1,1,1,1,1,0,0,1,1,0,0,1,0,0,0,0,0],
    "MSG5": [1,1,1,1,1,1,1,1,1,1,0,1,0,0,0,0,0,0,1,0,1,1],
    "MSG6": [1,1,1,1,1,1,1,1,1,1,0,1,0,0,0,0,0,1,1,1,1,1],
    "MSG7": [1,1,1,1,1,1,1,1,1,1,0,1,0,0,0,0,0,0,0,0,0,1],
    "MSG8": [1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,1],
    "SEL":  [1,0,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0],
    "ID":   [1,0,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0],
    "AIR":  [1,0,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0],
    "STA":  [1,0,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0],
    "CLK":  [1,0,1,0,0,0,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0]
}

class Message(IntEnum):
    messageType = 0
    transmissionType = 1
    sessionID = 2
    aircraftID = 3
    hexIdent = 4
    flightID = 5
    dataMessageGenerated = 6
    timeMessageGenerated = 7

    dateMessageLogged = 8
    timeMessageLogged = 9
    
    #non standard messages
    callsign = 10
    altitude = 11
    groundSpeed = 12
    track = 13
    lat = 14
    lon = 15
    verticalRate = 16
    squawk = 17
    alert = 18
    emergency = 19
    SPI = 20
    isOnGround = 21



def createJson(data, flag):

    message = {}
    #### non flagged
    dt = datetime.strptime(data[Message.dataMessageGenerated] + " " + data[Message.timeMessageGenerated], "%Y/%m/%d %H:%M:%S.%f")
    message['time'] = dt.timestamp()

    
    message['icao24'] = data[Message.hexIdent]
    ####

    message['lat'] = float(data[Message.lat]) if (flag[Message.lat] and data[Message.lat] != '') else None
    message['lon'] = float(data[Message.lon]) if (flag[Message.lon] and data[Message.lon] != '') else None
    message['velocity'] = float(data[Message.groundSpeed]) if (flag[Message.groundSpeed] and data[Message.groundSpeed] != '') else None
    
    message['heading'] = float(data[Message.track]) if (flag[Message.track] and data[Message.track] != '') else None

    message['vertrate'] = float(data[Message.verticalRate]) if (flag[Message.verticalRate] and data[Message.verticalRate] != '') else None
    message['callsign'] = data[Message.callsign] if (flag[Message.callsign] and data[Message.callsign] != '') else None
    message['onground'] = bool(int(data[Message.isOnGround])) if (flag[Message.isOnGround] and data[Message.isOnGround] != '') else None
    message['alert'] = bool(int(data[Message.alert])) if (flag[Message.alert] and data[Message.alert] != '') else None
    message['spi'] = bool(int(data[Message.SPI])) if (flag[Message.SPI] and data[Message.SPI] != '') else None
    message['squawk'] = float(data[Message.squawk]) if (flag[Message.squawk] and data[Message.squawk] != '') else None

    message['baroaltitude'] = None
    message['geoaltitude'] = float(data[Message.altitude]) if (flag[Message.altitude] and data[Message.altitude] != '') else None 

    
    
    dt = datetime.strptime(data[Message.dateMessageLogged] + " " + data[Message.timeMessageLogged], "%Y/%m/%d %H:%M:%S.%f")

    message['lastposupdate'] = dt.timestamp() if (flag[Message.lon] and data[Message.lon] != '' and flag[Message.lat] and data[Message.lat] != '') else None
    message['lastcontact'] = dt.timestamp()
    return(json.dumps(message))




def main():

    nc_command = ["nc", hostname, str(port)]

    # Create a Popen object, redirecting stdout to a pipe
    process = subprocess.Popen(nc_command, stdout=subprocess.PIPE)

    # Read data line by line from Netcat's stdout
    for line in process.stdout:
        #print(f"Received: {line.decode().strip()}")

        try:
            decodedLine = line.decode().strip()
            messagesList = decodedLine.split(",")
            messageType = messagesList[Message.messageType]
            transmissionType = messagesList[Message.transmissionType] 

            json = createJson(messagesList, roles[(messageType+transmissionType)])
            producer.send(PRODUCER_TOPIC, value=json)
        except:
            continue
    
    #print(str(messagesList[Message.groundSpeed])+ '\n')
    producer.flush()
    process.wait()

def test():
    with open('datafeed.csv') as f:
        a = [{k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)]

    for row in a:
        jsonResult = json.dumps(row)
        print(jsonResult)
        producer.send(PRODUCER_TOPIC, value=jsonResult)

    producer.flush()


if __name__ == "__main__":
    if TESTMODE:
        test()
    else:
        main()