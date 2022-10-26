import paho.mqtt.client as mqtt
from dataclasses import dataclass
import numpy as np


def on_connect(client, userdata, flags, rc):
    print(f"Connected wth result {rc}")
    client.subscribe("Magnetometer")

@dataclass 
class MagSample:
    timestamp: np.int64
    data: np.ndarray

    @classmethod
    def from_buf(cls, buf):
        timestamp = np.frombuffer(buf[:8], dtype=np.int64, count=1).item()
        data = np.frombuffer(buf[8:], dtype=np.float64, count=4)
        return cls(timestamp, data)

recieved = []

def on_message(client, userdata, msg):
    if msg.topic == "Magnetometer":
        data = MagSample.from_buf(msg.payload)
    else:
        data = msg.payload
    
    global recieved
    recieved.append(data)
    print(data)
    if len(recieved) % 6 == 0:
        deltas = []
        for i in range(len(recieved)-1):
            deltas.append(recieved[i+1].timestamp - recieved[i].timestamp)

        print(f"deltas: {deltas}")



client = mqtt.Client("MagnetometerRecv")
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883)
client.loop_forever()