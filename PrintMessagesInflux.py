import paho.mqtt.client as mqtt
from dataclasses import dataclass
import numpy as np
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import datetime
from ftpsync.targets import FsTarget
from ftpsync.ftp_target import FTPTarget
from ftpsync.synchronizers import DownloadSynchronizer, UploadSynchronizer
from configparser import ConfigParser
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import sys

# NOTE(cmo): Set this to False on deployment
LocalFsTest = False

ConfigPath = f"{os.environ['HOME']}/.config/magnetometer/server.conf"
InfluxBucketTemplate = "magnetometer{year:d}"
InfluxTag = "Magnetometer"
MqttTopic = "Magnetometer"

root_logger = logging.getLogger()
try:
    handler = TimedRotatingFileHandler(
        "/var/log/magnetometer-handler/log",
        when="D",
        backupCount=5
    )
except PermissionError as e:
    root_logger.error("Can't create logging file, due to permissions errors.")
    sys.exit(1)

root_logger.addHandler(handler)

Conf = ConfigParser()
Conf.read(ConfigPath)


# NOTE(cmo): Some code based on Sean Leavey's original magnetometer FTP/logging
# code https://github.com/acrerd/magnetometer/blob/master/magnetometer/ftp.py
# The FTP stuff will be replaced with rsync stuff once we have an account.

def on_connect(client, userdata, flags, rc):
    client.subscribe(MqttTopic)

@dataclass
class MagSample:
    timestamp: np.int64
    data: np.ndarray

    @classmethod
    def from_buf(cls, buf):
        timestamp = np.frombuffer(buf[:8], dtype=np.int64, count=1).item()
        data = np.frombuffer(buf[8:], dtype=np.float64, count=4)
        return cls(timestamp, data)

    def text_repr(self, midnight):
        if midnight is None:
            midnight = 0

        return f"{int(self.timestamp - midnight)} {self.data[0]} {self.data[1]} {self.data[2]} {self.data[3]}"

class MagnetometerDataMiddleLayer:
    def __init__(self, influx_client, influx_bucket, influx_tag):
        self.influx_client = influx_client
        self.influx_bucket = influx_bucket
        self.bucket_is_template = '{' in influx_bucket and '}' in influx_bucket
        self.influx_tag = influx_tag
        self.write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        if not os.path.exists(Conf["magnetometer"]["local_dir"]):
            os.makedirs(Conf["magnetometer"]["local_dir"])
            os.chmod(Conf["magnetometer"]["local_dir"], 0o1777)
        self.local_dir = FsTarget(Conf["magnetometer"]["local_dir"])
        if LocalFsTest:
            if not os.path.exists("/tmp/fake-magnetometer-remote"):
                os.makedirs("/tmp/fake-magnetometer-remote")
                os.chmod("/tmp/fake-magnetometer-remote", 0o1777)
            self.remote_target = FsTarget("/tmp/fake-magnetometer-remote")
        else:
            self.remote_target = FTPTarget(
                path=Conf["ftp"]["remote_dir"],
                host=Conf["ftp"]["host"],
                port=int(Conf["ftp"]["port"]),
                username=Conf["ftp"]["username"],
                password=Conf["ftp"]["password"],
                timeout=120,
            )
        download_opts = {
            "resolve": "remote",
            "match": "*" + self.filename_from_date(datetime.datetime.utcnow()),
        }
        self.ftp_downloader = DownloadSynchronizer(self.local_dir, self.remote_target, download_opts)
        self.ftp_uploader = UploadSynchronizer(self.local_dir, self.remote_target, {"resolve": "local"})
        self.sync_files_from_server()
        self.prev_sync_time = time.time()
        self.influx_point_list = []

    def bucket_name(self, data):
        bucket = self.influx_bucket
        if self.bucket_is_template:
            bucket = bucket.format(year=datetime.datetime.fromtimestamp(data.timestamp / 1000).year)
        return bucket

    def to_influx_bucket_point(self, data):
        bucket = self.bucket_name(data)
        p = Point(bucket).tag("instrument", self.influx_tag).field("east-west", data.data[0]).field("north-south", data.data[1]).field("up-down", data.data[2]).field("temperature", data.data[3]).time(data.timestamp, WritePrecision.MS)
        return bucket, p

    def submit_influx_point_list(self):
        # NOTE(cmo): For batching.
        buckets, points = zip(*self.influx_point_list)
        # NOTE(cmo): Do a batched write if they're all going to the same bucket.
        if all([b == buckets[0] for b in buckets]):
            self.write_api.write(bucket=buckets[0], record=points)
        else:
            for b, p in self.influx_point_list:
                self.write_api.write(bucket=b, record=p)

        self.influx_point_list = []

    def submit_reading_influx(self, data):
        bucket = self.bucket_name
        p = Point(bucket).tag("instrument", self.influx_tag).field("east-west", data.data[0]).field("north-south", data.data[1]).field("up-down", data.data[2]).field("temperature", data.data[3]).time(data.timestamp, WritePrecision.MS)
        self.write_api.write(bucket=bucket, record=p)

    def submit_reading_text(self, data):
        data_time = datetime.datetime.fromtimestamp(float(data.timestamp) / 1000)
        midnight_timestamp = data_time.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000

        file_path = os.path.join(Conf["magnetometer"]["local_dir"], self.filename_from_date(data_time))
        with open(file_path, 'a') as f:
            f.write(data.text_repr(midnight_timestamp) + "\n")

    def sync_files_from_server(self):
        self.ftp_downloader.run()

    def sync_files_to_server(self):
        self.ftp_uploader.run()
        self.prev_sync_time = time.time()

    def handle_mqtt_message(self, mag_msg):
        data = MagSample.from_buf(mag_msg.payload)

        self.influx_point_list.append(self.to_influx_bucket_point(data))

        self.submit_reading_text(data)

        # NOTE(cmo): Messages come through in batches of 4.
        if len(self.influx_point_list) >= 4:
            self.submit_influx_point_list()
            # NOTE(cmo): Don't do an FTP sync unless we're at the end of a batch
            if time.time() - self.prev_sync_time > float(Conf["ftp"]["sync_time"]):
                self.sync_files_to_server()

    @staticmethod
    def filename_from_date(t):
        return f"{t.strftime('%Y-%m-%d')}.txt"

if __name__ == '__main__':

    database = InfluxDBClient.from_config_file(ConfigPath)
    data_handler = MagnetometerDataMiddleLayer(database, InfluxBucketTemplate, InfluxTag)

    def on_message(client, userdata, msg):
        if msg.topic == MqttTopic:
            data_handler.handle_mqtt_message(msg)


    # NOTE(cmo): clean_session=False indicates that we are a "durable" client,
    # and messages should be preserved for us.
    client = mqtt.Client("MagnetometerRecv", clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883)
    client.loop_forever()
