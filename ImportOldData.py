from dataclasses import dataclass
import numpy as np
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import datetime
from configparser import ConfigParser
import os
from os import path


ConfigPath = f"{os.environ['HOME']}/.config/magnetometer/server.conf"
InfluxBucket = "observatory"
InfluxTag = "MagnetometerOld"
ImportPath = "/OldMagData/magnetometer/"
StartImportFrom = '2018-10-01'

Conf = ConfigParser()
Conf.read(ConfigPath)

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

    def to_point(self, influx_bucket, influx_tag):
        p = Point(influx_bucket).tag("instrument", influx_tag).field("east-west", self.data[0]).field("north-south", self.data[1]).field("up-down", self.data[2]).field("temperature", self.data[3]).time(self.timestamp, WritePrecision.MS)
        return p

    def submit_reading_influx(self, influx_bucket, influx_tag, write_api):
        p = self.to_point(influx_bucket, influx_tag)
        write_api.write(bucket=influx_bucket, record=p)

    @classmethod
    def from_array_line(cls, line, midnight):
        timestamp = np.int64(line[0] + midnight)
        data = line[1:]
        return cls(timestamp, data)

influx_client = InfluxDBClient.from_config_file(ConfigPath)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

if __name__ == '__main__':
    import_files = [ImportPath + f for f in sorted(os.listdir(ImportPath)) if f.endswith('.txt')]
    if StartImportFrom is not None:
        for i, f in enumerate(import_files):
            if StartImportFrom in f:
                break
        else:
            raise ValueError(f"StartImportFrom ({StartImportFrom}) value not found.")
        import_files = import_files[i:]

    for file in import_files:
        midnight = datetime.datetime.fromisoformat(f'{path.splitext(path.basename(file))[0]}T00:00:00+00:00').timestamp() * 1000
        data = np.genfromtxt(file)
        file_samples = []
        for line_idx in range(data.shape[0]):
            sample = MagSample.from_array_line(data[line_idx, :], midnight)
            file_samples.append(sample.to_point(InfluxBucket, InfluxTag))
        write_api.write(bucket=InfluxBucket, record=file_samples)

        print(f" => File '{file}' done.")
